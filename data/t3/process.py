# -*- coding:utf-8 -*-
# __author__ = 'xianghai'


def read_pair(path):
    ret = []
    with open(path, mode='r', encoding='utf-8') as fi:
        l = [i.strip('\n').split('\t') for i in fi.readlines()]
        for i in l:
            ret.append({
                'lq': i[1].replace(' ', '', -1),
                'rq': i[2].replace(' ', '', -1),
                'index': i[0]
            })
        return ret

if __name__ == '__main__':
    dev = read_pair('dev.txt')
    test = read_pair('test.txt')
    train = read_pair('train.txt')

    print(len(dev) + len(test) + len(train))

    for i in train:
        if i['lq'] == '集合资管计划产品概要':
            print(i)

    # lqs = set()
    # [lqs.add(j['lq']) for j in dev]
    # [lqs.add(j['lq']) for j in test]
    # [lqs.add(j['lq']) for j in train]
    #
    # with open('qs_in_repo.txt', mode='w', encoding='utf-8') as fo:
    #     index = 1
    #     for q in lqs:
    #         print(index, q)
    #         fo.write("%d %s\n" % (index, q))
    #         index += 1
