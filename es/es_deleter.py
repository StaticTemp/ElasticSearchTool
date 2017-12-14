# -*- coding:utf-8 -*-
# __author__ = 'xianghai'
import json
import time

import urllib3

from es import conf
from es.es_updater import create_ext_key

HTTP = urllib3.PoolManager()


def delete_from_es(qa_lst, batch_len=1000):
    """
    删除 elasticsearch 中的问答对数据
    :param qa_lst:
            "tenant-id": tenant_id,
            "qa-id": qa_id,
            "qa-id-ext": conf.QA_ID_EXT
    :param batch_len:
    :return:
    """
    actions = list()
    s = time.time()
    for index, item in enumerate(qa_lst):
        tenant_id = item["tenant-id"]
        qa_id = item["qa-id"]
        qa_id_ext = item["qa-id-ext"]

        op = {"delete": {"_index": conf.ES_INDEX_NAME, "_type": tenant_id, "_id": conf.ES_QA_ID % (qa_id, qa_id_ext)}}
        actions.append("%s\n" % (json.dumps(op),))

        if (index > 0 and index % batch_len == 0) or index >= len(qa_lst) - 1:
            r = HTTP.request("POST", "http://%s:%s/_bulk?pretty=true" % (conf.ES_HOST, conf.ES_PORT),
                             body="".join(actions).encode("utf-8"))
            try:
                if r.status == 200:
                    print("批量删除耗时: %s %s %s", time.time() - s, "es bulk info, success size:",
                                len(json.loads(r.data.decode())["items"]))
                else:
                    print("delete error (from es): %s", r.data.decode())
            except Exception as e:
                print("delete error (unknown): %s", e)
            actions.clear()
            s = time.time()

    return True


def delete(qa_list, batch_len=1000):
    """
    删除内存和REDIS中的数据
    :param<list<dict>> qa_list:
        param<number> tenant-id: 域ID
        param<number> qa-id: 问答对ID

    :param batch_len: 批量删除redis数值限制
    :return<boolean>: True/False
    """
    qa_list_len = len(qa_list)

    batch_lst = []
    for index, qa in enumerate(qa_list):
        tenant_id = qa["tenant-id"]
        qa_id = qa["qa-id"]
        like_questions = qa["like-questions"]

        batch_lst.append({
            "tenant-id": tenant_id,
            "qa-id": qa_id,
            "qa-id-ext": conf.QA_ID_EXT
        })

        for like_question in like_questions:
            batch_lst.append({
                "tenant-id": tenant_id,
                "qa-id": qa_id,
                "qa-id-ext": create_ext_key(tenant_id, qa_id, like_question)
            })

        if index % batch_len == 0 or index == qa_list_len - 1:
            delete_from_es(batch_lst)
            batch_lst.clear()

    return True


def delete_like_question(tenant_id, qa_id, questions):
    """
    删除相似问题对
    :param tenant_id:
    :param qaid:
    :param question:
    :return: None
    """
    print("delete like question>>: tenant-id: %s, qa-id: %s" % (tenant_id, qa_id))
    qa_lst = list()
    for question in questions:
        qa_lst.append(
            {
                "tenant-id": tenant_id,
                "qa-id": qa_id,
                "qa-id-ext": create_ext_key(tenant_id, qa_id, question)
            }
        )

    delete_from_es(qa_lst)


def delete_index(index_name):
    """
    删除索引，可以用作删除数据库
    :param index_name: == tenant-id
    :return:
    """
    r = HTTP.request("DELETE", "%s/%s" % (conf.ES_URL, index_name))
    print("删除索引 %s", r.data.decode())
