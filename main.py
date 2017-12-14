# -*- coding:utf-8 -*-
# __author__ = 'xianghai'
import json
from pprint import pprint

from es.es_getter import init, init_index
from es.es_search import full_text_search
from es.es_updater import analyze_sentence_from_ltp, analyze_sentence_from_es, update

"""
qa_list[]:
    param<number> tenant-id: 域ID
    param<number> qa-id: 问答对ID
    param<string> raw-question: 原问题
    param<string> raw-answer: 原答案, 是一个json结构体{"answer": "以前的answer",         #一定要填写的内容}
    param<string> content_type:text
    param<string> like-questions:
batch_len: number

"""

def put_t2_es():
    qa_list= []
    with open('data/t2.txt', mode='r', encoding='utf-8') as fi:
        lines = [i.strip('\n').split('\t') for i in fi.readlines()]
        for l in lines:
            tmp = {
                'tenant-id': 2,
                'qa-id': int(l[0]),
                'raw-question': l[1],
                'raw-answer': json.dumps({'answer': l[1] + ': ' + l[3]}, ensure_ascii=False),
                'content_type': 'text',
                'like-questions': []
            }
            qa_list.append(tmp)
    for i in qa_list:
        print(i)
    update(qa_list, len(qa_list))


def put_t3_es():
    qa_list= []
    with open('data/t3/qs_in_repo.txt', mode='r', encoding='utf-8') as fi:
        lines = [i.strip('\n').split(' ') for i in fi.readlines()]
        for l in lines:
            tmp = {
                'tenant-id': 3,
                'qa-id': int(l[0]),
                'raw-question': l[1],
                'raw-answer': json.dumps({'answer': 'A is: ' + l[1]}, ensure_ascii=False),
                'content_type': 'text',
                'like-questions': []
            }
            qa_list.append(tmp)
    for i in qa_list:
        print(i)
    update(qa_list, len(qa_list))


def put_label_es(tid, label_data_lst):
    """
    :param tid: int， 新的知识库id
    :param label_data_lst:
    :return:
    """
    qa_list = []
    for index, row in enumerate(label_data_lst):
        if row == []:
            continue
        # try:
        tmp = {
            'tenant-id': tid,
            'qa-id': index,
            'raw-question': row["uq"],
            'raw-answer': json.dumps({'answer': 'A is: ' + row["uq"]}, ensure_ascii=False),
            'content_type': 'text',
            'like-questions': []
        }
        qa_list.append(tmp)
        # except Exception as e:
        #     pprint(e)
        #     continue
    print("tid %d is loaded to es, total main questions is %d" % (tid, len(qa_list)))
    update(qa_list, len(qa_list))


if __name__ == '__main__':
    # put_t3_es()
    # init(0)
    # init_index('ES_INDEX_NAME')
    xx = full_text_search(4, "钢琴学多久可以考级?考级是如何进行的？")
    print(xx)


