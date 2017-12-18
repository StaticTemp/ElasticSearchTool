# -*- coding: utf-8 -*-

"""
被引用文件1
算法服务配置信息
"""

import json

# elastic search 配置信息
# ES_HOST = "39.106.49.104"
ES_HOST = "192.168.240.3"
ES_PORT = 9200
ES_URL = "http://%s:%s" % (ES_HOST, ES_PORT)

ES_SETTINGS = {
    "index": {
        "analysis": {
            "analyzer": {
                "ik-smart-with-synonym": {
                    "tokenizer": "ik_smart",
                    "filter": ["synonym"]
                },
                "ik-max-word-with-synonym": {
                    "tokenizer": "ik_max_word",
                    "filter": ["synonym"]
                },
                "whitespace-with-synonym": {
                    "tokenizer": "whitespace",
                    "filter": ["synonym"]
                }
            },
            "filter": {
                "synonym": {
                    "type": "synonym",
                    "synonyms_path": "analysis/synonym_es.txt",
                    #"synonyms": [
                    #    "nihao,oahin"
                    #],
                    "ignore_case": True
                }
            }
        },
        "similarity": {
            "qa-bm25": {
                #显示设置BM25参数，不建议修改
                "type": "BM25",
                "b": "0.75",
                "k1": "1.2"
            }
        }
    }
}

ES_MAPPINGS_DEFAULT = {
    "_default_": {
        "_all": {"enabled": False},
        "properties": {
            "raw-question": {
                "type": "text",
                "norms": {"enabled": True},
                "analyzer": "ik_smart",
                # https://www.elastic.co/guide/en/elasticsearch/reference/current/similarity.html
                "similarity": "qa-bm25",
                "fields": {
                    "imw": {  # ik最细粒度
                        "type": "text",
                        "analyzer": "ik_max_word"
                    },
                    "ws": {  # 空格分词
                        "type": "text",
                        "analyzer": "whitespace"
                    }
                }
            },
            "raw-answer": {
                "type": "text",
                "index": False
            },
            "tenant-id": {
                "type": "long"
            },
            "qa-id": {
                "type": "long"
            },
            "qa-id-ext": {
                "type": "keyword",
                "index": False
            },
            "content-type": {
                "type": "keyword",
                "index": False
            },
            "raw-question-suggest": {
                "type": "completion",
                "contexts": [
                    {
                        "name": "tenant-type",
                        "type": "category",
                        "path": "c-tenant-path"
                    }
                ]
            }
        }
    }
}

# ES name，数据写操作(别名或者真实索引名称)，<正常写操作>和<手动同步>，只能取其一进行，否则会造成数据错误
ES_INDEX_NAME = "qa-pair"
# ES name，数据读操作，别名
ES_INDEX_QUERY_NAME = "qa-pair"
# 主问答信息默认扩展值
QA_ID_EXT = "0"
# ES中真实的问答对ID，由 “主问题ID” 和 “相似问题ID” 前后两部分组成，如果是主问题，则后部分指定为QA_ID_EXT
ES_QA_ID = "%s-%s"
# LTP segment info
LTP_SERVER_URL = "http://192.168.240.3:9088/ltp"

PA_HOST = "192.168.240.3:9997"

