
import re
import json

import urllib3
from es import es_updater
from es import conf
from es import question_classification_interface
import numpy as np


def pre_sentence(seg_lst, pos_lst, lower=True, ignore_punctuation=True, ignore_stopword=True, with_pos=True):
    """
    预处理句子
    :param seg_lst: 句子分词结果
    :param lower:
    :param ignore_punctuation:
    :param ignore_stopword:
    :param with_pos:
    :return: [句子处理结果, 词性标注结果(非必须)]
        样例如下：
        问题: "今天天气怎么样？HELLO world，嗯，Oh no！@# ~"
        结果: [('今天', '天气', 'hello', 'world', 'oh', 'no') ('nt', 'n', 'ws', 'ws', 'ws', 'ws')]
    """

    seg_pos_pair_lst = list(zip(seg_lst, pos_lst))

    # 小写
    if lower:
        seg_pos_pair_lst = map(lambda pair: (pair[0].lower(),) if len(pair) <= 1 else (pair[0].lower(), pair[1]),
                               seg_pos_pair_lst)

    # 去标点
    if ignore_punctuation:
        seg_pos_pair_lst = filter(lambda pair: False if is_punctuation(pair[0]) else True, seg_pos_pair_lst)

    # 去停用词
    if ignore_stopword:
        seg_pos_pair_lst = filter(lambda pair: False if is_stopword(pair[0]) else True, seg_pos_pair_lst)

    result = list(seg_pos_pair_lst)
    if len(result) == 0:
        if with_pos:
            return [(), ()]
        else:
            return [()]
    else:
        return list(zip(*result))


def is_punctuation(
        word,
        pattern=re.compile(r"^[\~!@#\$%\^&\*\(\)\-_\+=\{\}\[\]\|\\:;\"'<,>\.\?/～！￥…×（）—【】、：；“”‘’《》，。？]+$")
):
    """
    判断word是否属于标点
    :param word:
    :param pattern:
    :return:
    """
    if pattern.match(word):
        return True
    return False


def is_stopword(word):
    """
    判断是否是停用词
    :param word: string
    :return: True/False
    """
    if word in STOPWORDS:
        return True
    return False

def get_stopwords(file="./data/ext_stopword.dic"):
    """
    获取停用词
    :param file:
    :return:
    """
    with open(file, encoding='utf8') as f:
        lines = f.readlines()
        return set(map(lambda str: str.strip("\n"), lines))

# 加载停用词
try:
    STOPWORDS = get_stopwords()
except:
    STOPWORDS = get_stopwords("./es/data/ext_stopword.dic")


HTTP = urllib3.PoolManager()


def count_with_one_tenant(tenant_id):
    """
    统计一个tenant下的数据数量
    :param tenant_id:
    :return:
    """
    r = HTTP.request("GET", "%s/%s/%s/_count" % (conf.ES_URL, conf.ES_INDEX_NAME, tenant_id))
    if r.status != 200:
        # 如果ES发生错误，尽快让问题显现
        logger.info("es error.")
        return None
    else:
        data = json.loads(r.data.decode())
        return data["count"]


def full_text_search(tenant_id, raw_question, top_size=128, main_que_type_lst=[]):
    # 需要对es检索进行改造, 现在保证 所有在es里面的记录都含有 main_que_type字段  类型是int
    # if len(main_que_type_lst) < 1:    则是在tenant_id内的所有记录检索
    # else:                             则是在tenant_id内的 并且 main_que_type 在main_que_type_lst列表内的记录 进行检索

    """
    初步筛选
    :param tenant_id:
    :param raw_question:
    :param top_pct:
    :return:
    """
    print("full_text_search tid:%d que:%s" % (tenant_id, raw_question))
    seg_lst_from_i, pos_lst_from_i = es_updater.analyze_sentence_from_ltp(raw_question)

    seg_lst, pos_lst = pre_sentence(seg_lst_from_i, pos_lst_from_i) #在归一化es分数时用到

    seg_lst_from_i = json.dumps(seg_lst_from_i, ensure_ascii=False)
    pos_lst_from_i = json.dumps(pos_lst_from_i, ensure_ascii=False)

    intention_from_i = question_classification_interface.get_intention_all(raw_question)

    # 生成main_que_type限制集
    mqt_limit_lst = list()
    for number in main_que_type_lst:
        mqt_limit_lst.append({
                "term": {
                    # 经过实验，ES会对数字类型转换成string类型进行匹配，所以这个没有严格的类型要求。
                    # 如果这个字段不存在，并且外部传入限制条件时，无法查询出任何数据。
                    "main_que_type": number
                }
            })

    query_body = {
        "from": 0,
        "size": top_size,
        "min_score": 0.0,
        "explain": False,
        "sort": {
            "_score": {
                "order": "desc"
            }
        },
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "raw-question.imw": {
                                "query": raw_question,
                                "analyzer": "ik-max-word-with-synonym"
                            }
                        }
                    },
                    {
                        "bool": {
                            "should": mqt_limit_lst
                        }
                    }
                ]
            }
        }
    }

    print(query_body)

    r = HTTP.request("POST",
                     "%s/%s/%s/_search?search_type=dfs_query_then_fetch" % (conf.ES_URL, conf.ES_INDEX_QUERY_NAME, tenant_id),
                     body=json.dumps(query_body).encode("utf-8"))

    if r.status != 200:
        logger.info("es return error")
        return []

    es_res = json.loads(r.data.decode())
    es_qa_info = es_res["hits"]["hits"]
    # print(es_qa_info)
    es_res_len = len(es_qa_info)

    if es_res_len == 0:
        return []

    result_num = es_res_len  # int(np.ceil(es_res_len * top_pct))

    # 数据量太少的情况 特殊处理
    # if result_num < 3:
    #    result_num = es_res_len

    final_res = list()

    #### 尼玛防止结果重复, key raw-question, value qa-id, 对于同一个raw-question, 只保留qa-id最大的那个
    avoid_dup_map = dict()
    for e in es_qa_info[0: result_num]:
        _source = e["_source"]
        key_str = _source["raw-question"]
        value_id = _source["qa-id"]
        if key_str not in avoid_dup_map:
            avoid_dup_map[key_str] = value_id
        else:
            if value_id > avoid_dup_map[key_str]:
                avoid_dup_map[key_str] = value_id

    #只返回设置答案格式的内容，默认为全选。
    # content_type = conf_algorithm.get_value(tenant_id, "CONTENT_TYPE")

    for e in es_qa_info[0: result_num]:
        _source = e["_source"]

        key_str = _source["raw-question"]
        value_id = _source["qa-id"]

        main_que_type = 0
        if 'main_que_type' in _source:
            main_que_type = int(_source['main_que_type'])

        if not ((key_str in avoid_dup_map) and (value_id == avoid_dup_map[key_str])):
            continue

        # if _source["content-type"] not in content_type:
        #     print("filter content-type: " + _source["content-type"] + str(_source["raw-question"]))
        #     continue

        know_base_len = count_with_one_tenant(tenant_id)
        es_score_nor = es_score_normalize(e["_score"], know_base_len, len(seg_lst))

        #针对出来的答案是utf_8二进制码，尝试先解码在存入：20170811,qxh
        # try:
        #     raw_anwer = json.loads(_source["raw-answer"])
        # except:
        #     raw_anwer = _source["raw-answer"]

        element_dict = {
            "tenant-id": _source["tenant-id"],
            "qa-id": _source["qa-id"],
            "score": es_score_nor,
            "pre-question-from-i": seg_lst_from_i,
            "pos-from-i": pos_lst_from_i,
            "pre-question-from-m": _source["pre-ltp-question"],
            "pos-from-m": _source["pos-ltp"],
            "raw-question": _source["raw-question"],
            "raw-answer": _source["raw-answer"],
            "raw-question-from-i": raw_question,
            "content_type": _source["content-type"],
            "qa-id-ext": _source["qa-id-ext"],
            "intention_from_i": json.dumps(intention_from_i, ensure_ascii=False),
            "intention_from_m": _source["intention_from_m"],
            "main_que_type": main_que_type,
        }

        # 紧急添加，以后删除 ----
        if element_dict["tenant-id"] == 46 and element_dict["content_type"] == "text":
            a = element_dict["raw-answer"]
            if a == "":
                continue
            try:
                aa = json.loads(a)
                if aa["answer"] == "":
                    continue
            except Exception as e:
                continue
        # 紧急添加，以后删除 ----

        final_res.append(element_dict)

    return final_res


def es_score_mean(s, N, n):
    """
    es平均分数
    """
    if N == None:
        print("warning: input tenant base len is None!")
        return 0
    if n < 1:
        print("warning: input word num are small than 1!")
        return 0
    if s < 0:
        print("warning: input es score is small than 0!")
        return 0

    return s / np.log(N) / n


def es_score_normalize(s, N, n):
    """
    es分数规整
    """
    score_mean_average = 0.5
    score_mean_variance = 0.5
    score_mean = es_score_mean(s, N, n)
    score_mean = (score_mean - score_mean_average) / score_mean_variance
    score_nor = 1 / (1 + np.exp(-score_mean))
    print("es score: " + str(score_nor))
    return score_nor


if __name__ == '__main__':
    xx = full_text_search(4, "钢琴学多久可以考级?考级是如何进行的？")
    print(xx)