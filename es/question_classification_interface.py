# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 09:55:18 2017

@author: qinxunhui
修改：由于意图参数可能变动，所以把所有意图封装成一个字典，那么外面入库和出库的代码就不用变动了。20170622
"""

import requests
from es import conf


def get_intention(sen, type):
    '''
    :param sen: str
    :param type: str，传入意图类型：qca,qcc,meta,pa.
    :return: intention，version
    qca:"reason_rules|amount_rules|advice_rules|definition_rules"：
    qcc:time_rules|location_rules|name_rules|price_rules|contact_rules
    meta: enumeration_rules, location_rules, name_rules, contact_rules, greetint_rules
    '''
    if len(type) == 0:
        print("there is no type to input in fun get_intention!")
        return "error", "error"

    if len(sen) == 0:
        print("there is no sen to input in fun get_intention!")
        return "unknown", "unknown"

    try:
        url = "http://%s/api/v1/sr?q=%s&grammar=%s" % (conf.PA_HOST, str(sen), str(type))
        rsp = requests.get(url)
        if rsp.status_code == 200:
            js = rsp.json()
            if js["code"] == 0:
                rule = js["rule"]
                return rule, js["version"]
            else:
                return "unknown", js["version"]
        else:
            print("warning: get_intention is failed, please check input type or server!")
            return 'error', 'error'

    except Exception as e:
        print(e)
        return 'error', 'error'


def get_intention_all(sen):
    '''
    返回意图的字典
    :param sen:
    :return: 意图
    '''
    inten_dic = {}
    inten_dic["version"] = "unknown"
    inten_dic_key = ["qca", "qcc"]
    for key in inten_dic_key:
        inten_dic[key], inten_dic["version"] = get_intention(sen, key)  # 向海说每个version都是一样的。
    return inten_dic


def intention_score_sub(qca_from_i, qcc_from_i, qca_from_m, qcc_from_m):
    '''
    :param qca_from_i:原问题的意图
    :param qcc_from_i:
    :param qca_from_m: 知识库问题意图
    :param qcc_from_m:
    :return: 意图匹配上的个数
    '''
    intension_score = 0
    qcc_flag = False
    qca_flag = False
    if qca_from_i == qca_from_m and qca_from_m != "unknown" and qca_from_m != "error":
        # print("qca_from_m are same! --" + qca_from_m)
        intension_score += 1
        qca_flag = True

    if qcc_from_i == qcc_from_m and qcc_from_m != "unknown" and qcc_from_m != "error":
        # print("qcc_from_m are same! --" + qcc_from_m)
        intension_score += 1
        qcc_flag = True

    if qca_from_i != qca_from_m:
        if qca_from_i != "unknown" and qca_from_i != "error" and qca_from_m != "unknown" and qca_from_m != "error":
            intension_score -= 1

    if qcc_from_i != qcc_from_m:
        if qcc_from_i != "unknown" and qcc_from_i != "error" and qcc_from_m != "unknown" and qcc_from_m != "error":
            intension_score -= 1

    # print(qcc_flag, qca_flag, qca_from_i, qcc_from_i, qca_from_m, qcc_from_m)
    if (qcc_flag or qca_flag) and qca_from_i == qca_from_m and qcc_from_i == qcc_from_m:
        intension_score += 1

    return intension_score


def intention_score(intention_i, intention_m):
    qca_from_i = intention_i["qca"]
    qcc_from_i = intention_i["qcc"]
    qca_from_m = intention_m["qca"]
    qcc_from_m = intention_m["qcc"]
    return intention_score_sub(qca_from_i, qcc_from_i, qca_from_m, qcc_from_m)


def intention_score_sen(sen1, sen2):
    inten_sen1 = get_intention_all(sen1)
    inten_sen2 = get_intention_all(sen2)

    return intention_score(inten_sen1, inten_sen2)


INTENTION_DIC_PATH = './result/intention_dic'
INTENTION_DIC = {}
INTENTION_DIC_CHANGE = False
import pickle


def get_intention_from_mem(sen):
    global INTENTION_DIC
    global INTENTION_DIC_PATH
    global INTENTION_DIC_CHANGE
    if len(INTENTION_DIC) == 0:
        print("start to load INTENTION_DIC from " + INTENTION_DIC_PATH)
        try:
            with open(INTENTION_DIC_PATH, 'rb') as file:
                INTENTION_DIC = pickle.load(file)
        except:
            INTENTION_DIC = {}
    if sen not in INTENTION_DIC.keys():
        INTENTION_DIC[sen] = get_intention_all(sen)
        INTENTION_DIC_CHANGE = True
    return INTENTION_DIC[sen]


def save_intention_to_file():
    global INTENTION_DIC
    global INTENTION_DIC_PATH
    global INTENTION_DIC_CHANGE
    if INTENTION_DIC_CHANGE and len(INTENTION_DIC) != 0:
        print("save INTENTION_DIC in " + INTENTION_DIC_PATH)
        with open(INTENTION_DIC_PATH, 'wb') as file:
            pickle.dump(INTENTION_DIC, file)

if __name__ == "__main__":
    sen1 = "仲裁申请需要哪些资料"
    sen2 = "申请仲裁需要提供什么资料"
    print(intention_score_sen(sen1, sen2))

