# -*- coding:utf-8 -*-
# __author__ = 'xianghai'
import hashlib
import json
import time

import urllib3
from lxml import etree

from es import conf
from es import question_classification_interface

HTTP = urllib3.PoolManager()


def analyze_sentence_from_ltp(sentence):
    """
    获取LTP分析结果
    :param sentence:
    :return:
    """
    data = "s=%s&x=n&t=all" % (sentence,)
    r = HTTP.request("POST", conf.LTP_SERVER_URL, body=data.encode("utf-8"))
    selector = etree.HTML(r.data)

    ltp_seg_lst = selector.xpath("//xml4nlp/doc/para/sent/word/@cont")
    ltp_pos_lst = selector.xpath("//xml4nlp/doc/para/sent/word/@pos")

    return list(ltp_seg_lst), list(ltp_pos_lst)


def analyze_sentence_from_es(raw_question, analyzer="ik_smart"):
    """
    分析句子，获取结果
    :param raw_question:
    :param analyzer:
    :return:分词结果
    """
    data = {
        "text": raw_question,
        "analyzer": analyzer
    }
    r = HTTP.request("POST", "%s/_analyze" % (conf.ES_URL,), body=json.dumps(data).encode("utf-8"))

    pre_res = list()
    if r.status == 200:
        j = json.loads(r.data.decode())
        for item in j["tokens"]:
            pre_res.append(item["token"])
    else:
        print("%s: status code not 200 %s", analyzer, r.data.decode())

    return pre_res


def update_to_es(qa_lst, batch_len=1000):
    """
    创建/更新ES数据
    :param qa_lst:
            "tenant-id": tenant_id,
            "qa-id": qa_id,
            "qa-id-ext": conf.QA_ID_EXT,
            "raw-question": raw_question,
            "raw-answer": raw_answer,
            "content-type": content_type,
    :param batch_len:
    :return:
    """

    actions = list()
    s = time.time()
    for index, item in enumerate(qa_lst):
        res = analyze_sentence_from_ltp(item["raw-question"])

        # ltp_word_lst = res["seg"]
        # ltp_pos_lst = res["pos"]
        ltp_word_lst = res[0]
        ltp_pos_lst = res[1]
        pre_ltp_question = json.dumps(ltp_word_lst, ensure_ascii=False)
        pos_ltp = json.dumps(ltp_pos_lst, ensure_ascii=False)
        # 问题意图分类
        intention_from_m = question_classification_interface.get_intention_all(item["raw-question"])

        intention_from_m = json.dumps(intention_from_m, ensure_ascii=False)
        suggestion = dict()
        suggestion["input"] = analyze_sentence_from_es(item["raw-question"], analyzer="ik_smart")
        suggestion["contexts"] = dict()
        suggestion["contexts"]["tenant-type"] = [str(item["tenant-id"])]

        op = {"index": {"_index": conf.ES_INDEX_NAME, "_type": item["tenant-id"],
                        "_id": conf.ES_QA_ID % (item["qa-id"], item["qa-id-ext"])}}
        op_data = {
            "tenant-id": item["tenant-id"],
            "qa-id": item["qa-id"],
            "qa-id-ext": item["qa-id-ext"],
            "raw-question": item["raw-question"],
            "raw-answer": item["raw-answer"],
            "content-type": item["content-type"],
            "pre-ltp-question": pre_ltp_question,
            "pos-ltp": pos_ltp,
            "raw-question-suggest": suggestion,
            "intention_from_m": intention_from_m,
            "main_que_type": item['main_que_type'],
        }

        actions.append("%s\n" % (json.dumps(op, ensure_ascii=False),))
        actions.append("%s\n" % (json.dumps(op_data, ensure_ascii=False),))

        # 实际ES批量导入判断
        if (index > 0 and index % batch_len == 0) or index >= len(qa_lst) - 1:
            r = HTTP.request("POST", "http://%s:%s/_bulk?pretty=true" % (conf.ES_HOST, conf.ES_PORT),
                             body="".join(actions).encode("utf-8"))
            try:
                if r.status == 200:
                    #logger.info("批量导入耗时: %s %s %s", time.time() - s, "es bulk info: success size", len(json.loads(r.data.decode())["items"]))
                    items = json.loads(r.data.decode())["items"]

                    for item in items:
                        print("batch test ===> type %s | id %s | status %s", item["index"]["_type"], item["index"]["_id"], item["index"]["status"])
                else:
                    print("update error (from es): %s", r.data.decode())
            except Exception as e:
                print("update error (unknown): %s", e)
            actions.clear()
            s = time.time()

    return True


def update(qa_list, batch_len=1000):
    """
    elasticsearch 数据更新维护 入口

    :param<list<dict>> qa_list:
        param<number> tenant-id: 域ID
        param<number> qa-id: 问答对ID
        param<string> raw-question: 原问题
        param<string> raw-answer: 原答案, 是一个json结构体  {
                                                                "answer": "以前的answer",         #一定要填写的内容
                                                                "url" : "url",                   #通用跳转地址, 选填
                                                                …… 其它扩展需要看文档
                                                            }
        param<string> content_type:
        param<string> like-questions:

    :param batch_len: number
    :return<boolean>: True/False
    """
    qa_list_len = len(qa_list)

    batch_lst = []
    for index, qa in enumerate(qa_list):
        tenant_id = qa["tenant-id"]
        qa_id = qa["qa-id"]
        raw_question = qa["raw-question"]
        raw_answer = json.dumps(qa["raw-answer"], ensure_ascii=False)
        content_type = qa["content_type"]
        like_questions = qa["like-questions"]
        main_que_type = 0
        if 'main_que_type' in qa:
            main_que_type = int(qa['main_que_type'])
        batch_lst.append({
            "tenant-id": tenant_id,
            "qa-id": qa_id,
            "qa-id-ext": conf.QA_ID_EXT,
            "raw-question": raw_question,
            "raw-answer": raw_answer,
            "content-type": content_type,
            "main_que_type": main_que_type,
        })

        for like_question in like_questions:
            batch_lst.append({
                "tenant-id": tenant_id,
                "qa-id": qa_id,
                "qa-id-ext": create_ext_key(tenant_id, qa_id, like_question),
                "raw-question": like_question,
                "raw-answer": raw_answer,
                "content-type": content_type,
                "main_que_type": main_que_type,
            })
        if (index > 0 and index % batch_len == 0) or index == qa_list_len - 1:
            update_to_es(batch_lst)
            batch_lst.clear()

    return True


def update_like_question(tenant_id, qa_id, questions):
    """
    ”创建/更新“相似问题对
    :param tenant_id:
    :param qaid:
    :param question:
    :return: None
    """
    print("update like question>>: tenant-id: %s, qa-id: %s" % (tenant_id, qa_id))

    r = HTTP.request("GET", "%s/%s/%s/%s" % (
        conf.ES_URL, conf.ES_INDEX_NAME, tenant_id, conf.ES_QA_ID % (qa_id, conf.QA_ID_EXT)))

    # 如果找不到，code会是404
    if r.status != 200:
        print("新增相似问题失败：%s", r.data.decode())
        return

    data = json.loads(r.data.decode())
    # raw_question = data["_source"]["raw-question"]
    raw_answer = data["_source"]["raw-answer"]
    content_type = data["_source"]["content-type"]
    main_que_type = data["_source"]["main_que_type"]
    qa_lst = list()
    for question in questions:
        qa_lst.append(
            {
                "tenant-id": tenant_id,
                "qa-id": qa_id,
                "qa-id-ext": create_ext_key(tenant_id, qa_id, question),
                "raw-question": question,
                "raw-answer": raw_answer,
                "content-type": content_type,
                "main_que_type": main_que_type
                # 这个key是下划线 ~~!
            }
        )

    update_to_es(qa_lst)


def create_ext_key(tenant_id, qa_id, question):
    """
    创建扩展ID，解决相似问题ID问题
    :param tenant_id:
    :param qa_id:
    :param question:
    :return: string
    """
    md5_str = "%s%s%s" % (tenant_id, qa_id, question)
    ext_id = hashlib.md5(md5_str.encode("utf-8")).hexdigest()
    return ext_id