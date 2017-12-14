import json

from es import load_similar_sen
from es import es_search
from main import put_label_es
from collections import OrderedDict
from openpyxl import Workbook
import pickle

def write_list_dic_to_excel(list_dic_name, excel_name='list_dic_to_excel.xlsx'):
    wb = Workbook()
    ws = wb.active

    keys = list(list_dic_name[0].keys())
    # ws.append(keys)

    for i in range(len(list_dic_name)):
        row_dic = []
        for j in keys:
            row_dic.append(list_dic_name[i][j])
        ws.append(row_dic)

    wb.save(excel_name)


def read_label_file(json_name):
    data_lst = []
    with open(json_name, 'r', encoding="utf8") as file:
        data_lst = file.readlines()
        print(json_name, len(data_lst))

    data_dic_lst = []
    for row in data_lst:
        if len(row) > 0:
            data_dic_lst.append(json.loads(row))
    return data_dic_lst


def gen_all_sen(json_index_lst = []):
    json_path = "./data/output/"
    label_data_lst = []
    for index in json_index_lst:
        json_name = json_path + "done_part_" + str(index) + ".json"
        label_data_lst.extend(read_label_file(json_name))

    sen_es = [row['uq'] for row in label_data_lst if len(row) > 0]
    sen_pos = []
    sen_neg = []

    sen_pair = []

    for row in label_data_lst:
        if len(row) > 0:
            pos = [e['simques'] for e in row['hit']]
            sen_pos.extend(pos)
            neg = [e['simques'] for e in row['nhit']]
            sen_neg.extend(neg)
            for e in row['hit']:
                pair_dic = OrderedDict()
                pair_dic["para"] = e['simques']
                pair_dic["groundtruth"] = row['uq']
                pair_dic["anwser"] = 'A is: ' + row["uq"]
                sen_pair.append(pair_dic)

    sen_all = sen_es + sen_pos + sen_neg
    sen_all = [row + "\n" for row in sen_all]

    temp = [str(e) for e in json_index_lst]
    file_name = "sen_label_" + "_".join(temp) + ".txt"
    with open(file_name, 'w', encoding='utf8') as file:
        file.writelines(sen_all)

    # 同义句去重，改写句不能够出现在原句列表中。
    sen_pair_new = []
    sen_gr_lst = [row["groundtruth"] for row in sen_pair]
    for row in sen_pair:
        if row["para"] in sen_gr_lst:
            print("ignore para -- ", row["para"])
        else:
            sen_pair_new.append(row)

    excel_name = "baidu_tid4" + "_".join(temp) + ".xlsx"
    write_list_dic_to_excel(sen_pair_new, excel_name)

    return sen_all, sen_pair


def load_label2es(new_tid, json_index_lst):
    # json_index_lst = [0, 30, 31, 60, 61]
    json_path = "./data/output/"
    label_data_lst = []
    for index in json_index_lst:
        json_name = json_path + "done_part_" + str(index) + ".json"
        label_data_lst.extend(read_label_file(json_name))
    put_label_es(new_tid, label_data_lst)


def get_es_result(tid, excel_name):
    """
    :param tid:
    :param excel_name:
    :return:
    """
    data = load_similar_sen.load_excel_para_sen(excel_name, -1)
    q1_lst = [row["paraphrase"] for row in data]
    q1_es_dic = {}
    for q1 in q1_lst[:5]:
        q1_es_dic[q1] = es_search.full_text_search(tid, q1)
    json_name = excel_name[:-5] + ".json"
    with open(json_name, 'w', encoding="utf8") as file:
        json.dump(q1_es_dic, file)

if __name__ == '__main__':
    # json_index_lst = [0, 1, 3, 30, 31, 32, 33, 34, 60, 61, 62]
    # load_label2es(4, json_index_lst)
    # sens = gen_all_sen(json_index_lst)
    # print(sens)

    get_es_result(4, "D:/evaluation/data/测试数据/baidu_tid4_0_1_3_30_31_32_33_34_60_61_62.xlsx")
    with open("D:/evaluation/data/测试数据/baidu_tid4_0_1_3_30_31_32_33_34_60_61_62.json", 'r', encoding='utf8') as file:
        data = json.load(file)
    print(data)
