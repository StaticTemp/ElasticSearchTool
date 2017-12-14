# -*- coding:utf-8 -*-
# __author__ = 'xianghai'
import json
import re

import urllib3

from es import conf

HTTP = urllib3.PoolManager()


def is_yun_tenant(tid):
    return tid >= 268435457


def init(level):
    if not (level == 0 or level == 1):
        print("level只能为0或1，传入值为：%s", level)
        return False
    r = get_index_real_name()
    if len(r) > 1 or len(r) <= 0:
        print("ES索引结构混乱，必须进行检查。")
        return False

    version = int(get_version_from_index_name(r[0][1]))
    # level == 0代表当前索引
    index_name = "%s-%s" % (conf.ES_INDEX_QUERY_NAME, version + level)

    # 创建新索引
    if init_index(index_name):
        if level == 1:
            conf.ES_INDEX_NAME = index_name
        return True
    else:
        return False


def init_index(index_name):
    """
    初始化索引，settings/mappings
    当索引存在时则初始化无效
    :param index_name: aliases qa-pair
    :return:
    """
    r = HTTP.request("HEAD", "%s/%s" % (conf.ES_URL, index_name))
    if r.status == 200:
        # 200 exists 404 not exists
        print("存在索引{%s}，无法创建", index_name)
        return False

    print("开始初始化索引 %s", index_name)
    data = {
        "settings": conf.ES_SETTINGS,
        "mappings": conf.ES_MAPPINGS_DEFAULT
    }

    r = HTTP.request("PUT", "%s/%s" % (conf.ES_URL, index_name), body=json.dumps(data).encode("utf-8"))
    print("初始化索引执行结束 %s %s", index_name, r.data.decode())

    if r.status == 200:
        return True
    else:
        return False


def aliases_index(old_index_name, new_index_name, alias_index_name):
    """
    重置索引别名
    :param old_index_name: 旧真实索引名称
    :param new_index_name: 新真实索引名称
    :param alias_index_name: 别名
    :return:
    """
    data = {
        "actions": [
            {
                "remove": {
                    "index": old_index_name,
                    "alias": alias_index_name
                }
            }
        ]
    }
    r = HTTP.request("POST", "http://%s:%s/_aliases?pretty=true" % (conf.ES_HOST, conf.ES_PORT), body=json.dumps(data).encode("utf-8"))
    print("删除索引别名结束：%s", r.data.decode())

    data = {
        "actions": [
            {
                "add": {
                    "index": new_index_name,
                    "alias": alias_index_name
                }
            }
        ]
    }
    r = HTTP.request("POST", "http://%s:%s/_aliases?pretty=true" % (conf.ES_HOST, conf.ES_PORT), body=json.dumps(data).encode("utf-8"))
    print("添加索引别名结束：%s", r.data.decode())


def get_index_real_name():
    """
    获取一定规则下的真实索引名称，异常情况下返回空LIST
    :return: 版本号和知识库真实存储名称
    """
    p = "%s-*" % (conf.ES_INDEX_QUERY_NAME,) #query name，稳定不变
    r = HTTP.request("GET", "http://%s:%s/_cat/indices/%s?h=i" % (conf.ES_HOST, conf.ES_PORT, p))

    if r.status == 200:
        indices_name = r.data.decode()

        if indices_name == "":
            r = [[0, "%s-0" % (conf.ES_INDEX_QUERY_NAME,)]]
            print("知识库为空，返回初始化默认值：%s", r)
            return r

        index_lst = indices_name.strip().split("\n")

        print("知识库索引信息：%s", index_lst)

        lst = list()
        for item in index_lst:
            v = get_version_from_index_name(item)
            lst.append([v, item])
        return sorted(lst, key=lambda e: e[0])
    else:
        print("ES异常！！！")
        return []


def get_version_from_index_name (name):
    """
    根据索引名称获取版本号
    :param name:
    :return:
    """
    p = re.compile(r"\d{1,}")
    # 当前索引版本号
    r = p.findall(name)
    if len(r) == 0:
        return -1

    return int(r[0])


def check_analyzer(tenant_id, analyzer_name):
    data = {
        "analyzer": analyzer_name,
        "text": "世界你好！hello world"
    }
    r = HTTP.request("POST", "%s/%s/_analyze" % (conf.ES_URL, tenant_id), body=json.dumps(data).encode("utf-8"))
    if r.status != 200:
        print("tenant_id: %s的分析器<%s>检查失败", tenant_id, analyzer_name)
        return False

    return True


def get_max_qaid(tenant_id):
    """
    得到知识库最大的qaid
    return >=0 succ, -1 fail
    :param tenant_id:
    :return:
    """
    query_body = {
        "from": 0,
        "size": 1,
        "stored_fields": [
            "qa-id"
        ],
        "sort": {
            "qa-id": {
                "order": "desc"
            }
        },
        "query": {
            "match_all": {}
        }

    }

    r = HTTP.request("POST", "%s/%s/%s/_search?" % (conf.ES_URL, conf.ES_INDEX_QUERY_NAME, tenant_id),
                     body=json.dumps(query_body).encode("utf-8"))

    ret_value = 0
    if r.status != 200:
        return ret_value

    es_res = json.loads(r.data.decode())

    es_qa_info = es_res["hits"]["hits"]
    es_res_len = len(es_qa_info)

    if es_res_len == 0:
        return ret_value

    if "_id" in es_qa_info[0]:
        ret_value = int(es_qa_info[0]["_id"])

    return ret_value


def get_qa_from_es_with_id(tenant_id, es_qa_id):
    """
    根据传入的ID获取问答对信息
    :param tenant_id:
    :param es_qa_id:
    :return:
    """
    print("获取主问题信息：%s %s", tenant_id, es_qa_id)
    r = HTTP.request("GET", "%s/%s/%s/%s" % (conf.ES_URL, conf.ES_INDEX_NAME, tenant_id, es_qa_id))

    if r.status != 200:
        return None

    jn = json.loads(r.data.decode())

    if jn.get("found"):
        return jn.get("_source")
    else:
        return None


def count_with_one_tenant(tenant_id):
    """
    统计一个tenant下的数据数量
    :param tenant_id:
    :return:
    """
    r = HTTP.request("GET", "%s/%s/%s/_count" % (conf.ES_URL, conf.ES_INDEX_NAME, tenant_id))
    if r.status != 200:
        # 如果ES发生错误，尽快让问题显现
        print("es error.")
        return None
    else:
        data = json.loads(r.data.decode())
        return data["count"]
