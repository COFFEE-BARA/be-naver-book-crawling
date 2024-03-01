
# 📚 Checkbara 서비스 소개

![image](https://github.com/COFFEE-BARA/crawler-kyobo-isbn/assets/65851554/aeecb4be-6f02-4bfd-a6e5-88df1ef87c8a)

![image](https://github.com/COFFEE-BARA/crawler-kyobo-isbn/assets/65851554/9ad000e0-5c95-4799-9582-2c49ecd5232b)


<br/>
<br/>


## 📹 시연영상



| <img width="250" src="https://github.com/COFFEE-BARA/crawler-kyobo-isbn/assets/65851554/a28f0cca-ae1f-46b9-a087-6ea18216bd9d"> | <img width="250" src="https://github.com/COFFEE-BARA/crawler-kyobo-isbn/assets/65851554/699d09a6-8691-4ca0-bab5-575340a3c34d"> | <img width="250" src="https://github.com/COFFEE-BARA/crawler-kyobo-isbn/assets/65851554/d4ac2392-57ff-406f-b03e-334c932d56ce"> |
| :---: | :---: | :---: |
| AI 책 추천 | 책 검색 & 서점 재고 확인 & 대출 가능 도서관 확인 | 키바나 통계보기 |


<br/>
<br/>


## 📡 발전 방향

![image](https://github.com/COFFEE-BARA/crawler-kyobo-isbn/assets/65851554/d18f754c-773d-43b9-b961-7931703aaeb8)


<br/>
<br/>


# 👥 팀원 소개

| <img width="165" alt="suwha" src="https://github.com/COFFEE-BARA/be-bookstore-stock/assets/72396865/19e01fac-5384-4ec7-98f1-9e1e613429b4"> | <img width="165" alt="yoonju" src="https://github.com/COFFEE-BARA/be-bookstore-stock/assets/72396865/fb0a14c6-2d02-4105-962e-4565663817cc"> | <img width="165" alt="yugyeong" src="https://github.com/COFFEE-BARA/be-bookstore-stock/assets/72396865/90b7268d-92e5-43d1-9da8-ae48afd9e8c1"> | <img width="165" alt="dayeon" src="https://github.com/COFFEE-BARA/be-bookstore-stock/assets/72396865/f19e65e6-0856-4b6a-a355-993ce83ddcb7"> |
| --- | --- | --- | --- |
| 🐼[유수화](https://github.com/YuSuhwa-ve)🐼 | 🐱[송윤주](https://github.com/raminicano)🐱 | 🐶[현유경](https://github.com/yugyeongh)🐶 | 🐤[양다연](https://github.com/dayeon1201)🐤 |
| Server / Data / BE | AI / Data / BE | Infra / BE / FE | BE / FE |



<br/>
<br/>


# ⚒️ 전체 아키텍처

![image](https://github.com/COFFEE-BARA/crawler-kyobo-isbn/assets/65851554/04a7f4e8-2c00-4084-88e2-e7ffd273187a)


<br/>
<br/>


# 💿 Dynamo DB table 구조도

## smwu_team_04_kyobo_cate_code_time

| crawling_time (PK) | static_number-crawling_time-index(SK) | payload |
| --- | --- | --- |
| 크롤링 시간 | 1 | 교보문고 카테고리 - 최신 책 출간일 리스트 |



<br/>
<br/>

# 🍭 Elastic stack index 구조도

## book-index

<details>
<summary>book-index mapping</summary>
<div markdown="1">

```
// book-index mapping

{
  "mappings": {
    "properties": {
      "Author": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "text",
            "analyzer": "author_analyzer"
          },
          "partial": {
            "type": "text",
            "analyzer": "edge_ngram_analyzer"
          }
        }
      },
      "DetailCategory": {
        "type": "keyword"
      },
      "ISBN": {
        "type": "keyword"
      },
      "ImageURL": {
        "type": "keyword"
      },
      "IndexContent": {
        "type": "text"
      },
      "Introduction": {
        "type": "text"
      },
      "MiddleCategory": {
        "type": "keyword"
      },
      "Price": {
        "type": "integer"
      },
      "PubDate": {
        "type": "date",
        "format": "yyyy-MM-dd"
      },
      "Publisher": {
        "type": "keyword"
      },
      "PublisherReview": {
        "type": "text"
      },
      "PurchaseURL": {
        "type": "keyword"
      },
      "Search": {
        "type": "text"
      },
      "Title": {
        "type": "text",
        "analyzer": "title_analyzer"
      },
      "Vector": {
        "type": "dense_vector",
        "dims": 768,
        "index": true,
        "similarity": "cosine"
      },
      "document": {
        "type": "object"
      },
      "id": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "index": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "pipeline": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      }
    }
  }
}
```

</div>
</details>


<details>
<summary> book-index settings </summary>
<div markdown="1">

```
//book -index settings
{
  "settings": {
    "index": {
      "routing": {
        "allocation": {
          "include": {
            "_tier_preference": "data_content"
          }
        }
      },
      "number_of_shards": "1",
      "provided_name": "book-index",
      "creation_date": "1708182319595",
      "analysis": {
        "filter": {
          "lowercase_filter": {
            "type": "lowercase"
          },
          "edge_ngram_filter": {
            "type": "edge_ngram",
            "min_gram": "1",
            "max_gram": "10"
          }
        },
        "analyzer": {
          "edge_ngram_analyzer": {
            "filter": [
              "edge_ngram_filter",
              "lowercase_filter"
            ],
            "type": "custom",
            "tokenizer": "nori_tokenizer_mine"
          },
          "author_analyzer": {
            "filter": [
              "lowercase_filter"
            ],
            "type": "custom",
            "tokenizer": "keyword"
          },
          "title_analyzer": {
            "filter": [
              "nori_readingform",
              "lowercase_filter",
              "nori_part_of_speech"
            ],
            "type": "custom",
            "tokenizer": "nori_tokenizer_mine"
          }
        },
        "tokenizer": {
          "nori_tokenizer_mine": {
            "type": "nori_tokenizer",
            "decompound_mode": "mixed"
          }
        }
      },
      "number_of_replicas": "2",
      "uuid": "okUbOg_pTJKVG2WO7e3rYQ",
      "version": {
        "created": "8500003"
      }
    }
  },
  "defaults": {
    "index": {
      "flush_after_merge": "512mb",
      "time_series": {
        "end_time": "9999-12-31T23:59:59.999Z",
        "start_time": "-9999-01-01T00:00:00Z",
        "es87tsdb_codec": {
          "enabled": "true"
        }
      },
      "final_pipeline": "_none",
      "max_inner_result_window": "100",
      "unassigned": {
        "node_left": {
          "delayed_timeout": "1m"
        }
      },
      "max_terms_count": "65536",
      "rollup": {
        "source": {
          "name": "",
          "uuid": ""
        }
      },
      "lifecycle": {
        "prefer_ilm": "true",
        "rollover_alias": "",
        "origination_date": "-1",
        "name": "",
        "parse_origination_date": "false",
        "step": {
          "wait_time_threshold": "12h"
        },
        "indexing_complete": "false"
      },
      "mode": "standard",
      "routing_partition_size": "1",
      "force_memory_term_dictionary": "false",
      "max_docvalue_fields_search": "100",
      "merge": {
        "scheduler": {
          "max_thread_count": "1",
          "auto_throttle": "true",
          "max_merge_count": "6"
        },
        "policy": {
          "merge_factor": "32",
          "floor_segment": "2mb",
          "max_merge_at_once_explicit": "30",
          "max_merge_at_once": "10",
          "max_merged_segment": "0b",
          "expunge_deletes_allowed": "10.0",
          "segments_per_tier": "10.0",
          "type": "UNSET",
          "deletes_pct_allowed": "20.0"
        }
      },
      "max_refresh_listeners": "1000",
      "max_regex_length": "1000",
      "load_fixed_bitset_filters_eagerly": "true",
      "number_of_routing_shards": "1",
      "write": {
        "wait_for_active_shards": "1"
      },
      "verified_before_close": "false",
      "mapping": {
        "coerce": "false",
        "nested_fields": {
          "limit": "50"
        },
        "depth": {
          "limit": "20"
        },
        "field_name_length": {
          "limit": "9223372036854775807"
        },
        "total_fields": {
          "limit": "1000"
        },
        "nested_objects": {
          "limit": "10000"
        },
        "ignore_malformed": "false",
        "dimension_fields": {
          "limit": "21"
        }
      },
      "source_only": "false",
      "soft_deletes": {
        "enabled": "true",
        "retention": {
          "operations": "0"
        },
        "retention_lease": {
          "period": "12h"
        }
      },
      "max_script_fields": "32",
      "query": {
        "default_field": [
          "*"
        ],
        "parse": {
          "allow_unmapped_fields": "true"
        }
      },
      "format": "0",
      "frozen": "false",
      "sort": {
        "missing": [],
        "mode": [],
        "field": [],
        "order": []
      },
      "priority": "1",
      "routing_path": [],
      "version": {
        "compatibility": "8500003"
      },
      "codec": "default",
      "max_rescore_window": "10000",
      "bloom_filter_for_id_field": {
        "enabled": "true"
      },
      "max_adjacency_matrix_filters": "100",
      "analyze": {
        "max_token_count": "10000"
      },
      "gc_deletes": "60s",
      "top_metrics_max_size": "10",
      "optimize_auto_generated_id": "true",
      "max_ngram_diff": "1",
      "hidden": "false",
      "translog": {
        "flush_threshold_age": "1m",
        "generation_threshold_size": "64mb",
        "flush_threshold_size": "10gb",
        "sync_interval": "5s",
        "retention": {
          "size": "-1",
          "age": "-1"
        },
        "durability": "REQUEST"
      },
      "auto_expand_replicas": "false",
      "fast_refresh": "false",
      "recovery": {
        "type": ""
      },
      "requests": {
        "cache": {
          "enable": "true"
        }
      },
      "data_path": "",
      "highlight": {
        "max_analyzed_offset": "1000000",
        "weight_matches_mode": {
          "enabled": "true"
        }
      },
      "look_back_time": "2h",
      "routing": {
        "rebalance": {
          "enable": "all"
        },
        "allocation": {
          "disk": {
            "watermark": {
              "ignore": "false"
            }
          },
          "enable": "all",
          "total_shards_per_node": "-1"
        }
      },
      "search": {
        "slowlog": {
          "level": "TRACE",
          "threshold": {
            "fetch": {
              "warn": "-1",
              "trace": "-1",
              "debug": "-1",
              "info": "-1"
            },
            "query": {
              "warn": "-1",
              "trace": "-1",
              "debug": "-1",
              "info": "-1"
            }
          }
        },
        "idle": {
          "after": "30s"
        },
        "throttled": "false"
      },
      "fielddata": {
        "cache": "node"
      },
      "look_ahead_time": "2h",
      "default_pipeline": "_none",
      "max_slices_per_scroll": "1024",
      "shard": {
        "check_on_startup": "false"
      },
      "xpack": {
        "watcher": {
          "template": {
            "version": ""
          }
        },
        "version": "",
        "ccr": {
          "following_index": "false"
        }
      },
      "percolator": {
        "map_unmapped_fields_as_text": "false"
      },
      "allocation": {
        "max_retries": "5",
        "existing_shards_allocator": "gateway_allocator"
      },
      "refresh_interval": "1s",
      "indexing": {
        "slowlog": {
          "reformat": "true",
          "threshold": {
            "index": {
              "warn": "-1",
              "trace": "-1",
              "debug": "-1",
              "info": "-1"
            }
          },
          "source": "1000",
          "level": "TRACE"
        }
      },
      "compound_format": "1gb",
      "blocks": {
        "metadata": "false",
        "read": "false",
        "read_only_allow_delete": "false",
        "read_only": "false",
        "write": "false"
      },
      "max_result_window": "10000",
      "store": {
        "stats_refresh_interval": "10s",
        "type": "",
        "fs": {
          "fs_lock": "native"
        },
        "preload": [],
        "snapshot": {
          "snapshot_name": "",
          "index_uuid": "",
          "cache": {
            "prewarm": {
              "enabled": "true"
            },
            "enabled": "true",
            "excluded_file_types": []
          },
          "repository_uuid": "",
          "uncached_chunk_size": "-1b",
          "delete_searchable_snapshot": "false",
          "index_name": "",
          "partial": "false",
          "blob_cache": {
            "metadata_files": {
              "max_length": "64kb"
            }
          },
          "repository_name": "",
          "snapshot_uuid": ""
        }
      },
      "queries": {
        "cache": {
          "enabled": "true"
        }
      },
      "shard_limit": {
        "group": "normal"
      },
      "warmer": {
        "enabled": "true"
      },
      "downsample": {
        "origin": {
          "name": "",
          "uuid": ""
        },
        "source": {
          "name": "",
          "uuid": ""
        },
        "status": "unknown"
      },
      "override_write_load_forecast": "0.0",
      "max_shingle_diff": "3",
      "query_string": {
        "lenient": "false"
      }
    }
  }
}
```


</div>
</details>



<br/>

## kyobo-isbn-cate-index


<details>
<summary> kyobo-isbn-cate-index mapping </summary>
<div markdown="1">

```
{
  "mappings": {
    "properties": {
      "category": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "crawling_date": {
        "type": "date"
      },
      "isbn": {
        "type": "keyword"
      }
    }
  }
}
```

</div>
</details>



<details>
<summary> kyobo-isbn-cate-index settings </summary>
<div markdown="1">


```
//settings

{
  "settings": {
    "index": {
      "routing": {
        "allocation": {
          "include": {
            "_tier_preference": "data_content"
          }
        }
      },
      "number_of_shards": "1",
      "provided_name": "kyobo-isbn-cate",
      "creation_date": "1708331140142",
      "number_of_replicas": "1",
      "uuid": "K0hRLifnSR2HRUqM0H9w3w",
      "version": {
        "created": "8500003"
      }
    }
  },
  "defaults": {
    "index": {
      "flush_after_merge": "512mb",
      "time_series": {
        "end_time": "9999-12-31T23:59:59.999Z",
        "start_time": "-9999-01-01T00:00:00Z",
        "es87tsdb_codec": {
          "enabled": "true"
        }
      },
      "final_pipeline": "_none",
      "max_inner_result_window": "100",
      "unassigned": {
        "node_left": {
          "delayed_timeout": "1m"
        }
      },
      "max_terms_count": "65536",
      "rollup": {
        "source": {
          "name": "",
          "uuid": ""
        }
      },
      "lifecycle": {
        "prefer_ilm": "true",
        "rollover_alias": "",
        "origination_date": "-1",
        "name": "",
        "parse_origination_date": "false",
        "step": {
          "wait_time_threshold": "12h"
        },
        "indexing_complete": "false"
      },
      "mode": "standard",
      "routing_partition_size": "1",
      "force_memory_term_dictionary": "false",
      "max_docvalue_fields_search": "100",
      "merge": {
        "scheduler": {
          "max_thread_count": "1",
          "auto_throttle": "true",
          "max_merge_count": "6"
        },
        "policy": {
          "merge_factor": "32",
          "floor_segment": "2mb",
          "max_merge_at_once_explicit": "30",
          "max_merge_at_once": "10",
          "max_merged_segment": "0b",
          "expunge_deletes_allowed": "10.0",
          "segments_per_tier": "10.0",
          "type": "UNSET",
          "deletes_pct_allowed": "20.0"
        }
      },
      "max_refresh_listeners": "1000",
      "max_regex_length": "1000",
      "load_fixed_bitset_filters_eagerly": "true",
      "number_of_routing_shards": "1",
      "write": {
        "wait_for_active_shards": "1"
      },
      "verified_before_close": "false",
      "mapping": {
        "coerce": "false",
        "nested_fields": {
          "limit": "50"
        },
        "depth": {
          "limit": "20"
        },
        "field_name_length": {
          "limit": "9223372036854775807"
        },
        "total_fields": {
          "limit": "1000"
        },
        "nested_objects": {
          "limit": "10000"
        },
        "ignore_malformed": "false",
        "dimension_fields": {
          "limit": "21"
        }
      },
      "source_only": "false",
      "soft_deletes": {
        "enabled": "true",
        "retention": {
          "operations": "0"
        },
        "retention_lease": {
          "period": "12h"
        }
      },
      "max_script_fields": "32",
      "query": {
        "default_field": [
          "*"
        ],
        "parse": {
          "allow_unmapped_fields": "true"
        }
      },
      "format": "0",
      "frozen": "false",
      "sort": {
        "missing": [],
        "mode": [],
        "field": [],
        "order": []
      },
      "priority": "1",
      "routing_path": [],
      "version": {
        "compatibility": "8500003"
      },
      "codec": "default",
      "max_rescore_window": "10000",
      "bloom_filter_for_id_field": {
        "enabled": "true"
      },
      "max_adjacency_matrix_filters": "100",
      "analyze": {
        "max_token_count": "10000"
      },
      "gc_deletes": "60s",
      "top_metrics_max_size": "10",
      "optimize_auto_generated_id": "true",
      "max_ngram_diff": "1",
      "hidden": "false",
      "translog": {
        "flush_threshold_age": "1m",
        "generation_threshold_size": "64mb",
        "flush_threshold_size": "10gb",
        "sync_interval": "5s",
        "retention": {
          "size": "-1",
          "age": "-1"
        },
        "durability": "REQUEST"
      },
      "auto_expand_replicas": "false",
      "fast_refresh": "false",
      "recovery": {
        "type": ""
      },
      "requests": {
        "cache": {
          "enable": "true"
        }
      },
      "data_path": "",
      "highlight": {
        "max_analyzed_offset": "1000000",
        "weight_matches_mode": {
          "enabled": "true"
        }
      },
      "look_back_time": "2h",
      "routing": {
        "rebalance": {
          "enable": "all"
        },
        "allocation": {
          "disk": {
            "watermark": {
              "ignore": "false"
            }
          },
          "enable": "all",
          "total_shards_per_node": "-1"
        }
      },
      "search": {
        "slowlog": {
          "level": "TRACE",
          "threshold": {
            "fetch": {
              "warn": "-1",
              "trace": "-1",
              "debug": "-1",
              "info": "-1"
            },
            "query": {
              "warn": "-1",
              "trace": "-1",
              "debug": "-1",
              "info": "-1"
            }
          }
        },
        "idle": {
          "after": "30s"
        },
        "throttled": "false"
      },
      "fielddata": {
        "cache": "node"
      },
      "look_ahead_time": "2h",
      "default_pipeline": "_none",
      "max_slices_per_scroll": "1024",
      "shard": {
        "check_on_startup": "false"
      },
      "xpack": {
        "watcher": {
          "template": {
            "version": ""
          }
        },
        "version": "",
        "ccr": {
          "following_index": "false"
        }
      },
      "percolator": {
        "map_unmapped_fields_as_text": "false"
      },
      "allocation": {
        "max_retries": "5",
        "existing_shards_allocator": "gateway_allocator"
      },
      "refresh_interval": "1s",
      "indexing": {
        "slowlog": {
          "reformat": "true",
          "threshold": {
            "index": {
              "warn": "-1",
              "trace": "-1",
              "debug": "-1",
              "info": "-1"
            }
          },
          "source": "1000",
          "level": "TRACE"
        }
      },
      "compound_format": "1gb",
      "blocks": {
        "metadata": "false",
        "read": "false",
        "read_only_allow_delete": "false",
        "read_only": "false",
        "write": "false"
      },
      "max_result_window": "10000",
      "store": {
        "stats_refresh_interval": "10s",
        "type": "",
        "fs": {
          "fs_lock": "native"
        },
        "preload": [],
        "snapshot": {
          "snapshot_name": "",
          "index_uuid": "",
          "cache": {
            "prewarm": {
              "enabled": "true"
            },
            "enabled": "true",
            "excluded_file_types": []
          },
          "repository_uuid": "",
          "uncached_chunk_size": "-1b",
          "delete_searchable_snapshot": "false",
          "index_name": "",
          "partial": "false",
          "blob_cache": {
            "metadata_files": {
              "max_length": "64kb"
            }
          },
          "repository_name": "",
          "snapshot_uuid": ""
        }
      },
      "queries": {
        "cache": {
          "enabled": "true"
        }
      },
      "shard_limit": {
        "group": "normal"
      },
      "warmer": {
        "enabled": "true"
      },
      "downsample": {
        "origin": {
          "name": "",
          "uuid": ""
        },
        "source": {
          "name": "",
          "uuid": ""
        },
        "status": "unknown"
      },
      "override_write_load_forecast": "0.0",
      "max_shingle_diff": "3",
      "query_string": {
        "lenient": "false"
      }
    }
  }
}
```


</div>
</details>




<br/>

## 데이터 관리전략


![image](https://github.com/COFFEE-BARA/crawler-kyobo-isbn/assets/65851554/10c4db70-82ec-4219-a9cb-77a2fe11e69b)

<br/>
<br/>


# 🏆 Tech Stack


## Programming language
<img src="https://img.shields.io/badge/go-00ADD8?style=for-the-badge&logo=go&logoColor=white"/>

<br/>

## DB

<img src="https://img.shields.io/badge/dynamoDB-4053D6?style=for-the-badge&logo=amazondynamodb&logoColor=white"> <img src="https://img.shields.io/badge/elastic-005571?style=for-the-badge&logo=elastic&logoColor=white">

<br/>

## Deploy & CI/CD
<img src="https://img.shields.io/badge/lambda-FF9900?style=for-the-badge&logo=awslambda&logoColor=white"/>  <img src="https://img.shields.io/badge/docker-2496ED?style=for-the-badge&logo=docker&logoColor=white"> <img src="https://img.shields.io/badge/ecr-FC4C02?style=for-the-badge&logo=ecr&logoColor=white"> <img src="https://img.shields.io/badge/codebuild-68A51C?style=for-the-badge&logo=codebuild&logoColor=white"> <img src="https://img.shields.io/badge/codepipeline-527FFF?style=for-the-badge&logo=codepipeline&logoColor=white"> 

<br/>

## Version Control System

<img src="https://img.shields.io/badge/github-181717?style=for-the-badge&logo=github&logoColor=white"> <img src="https://img.shields.io/badge/git-F05032?style=for-the-badge&logo=git&logoColor=white"> 

<br/>

## Communication Tool

<img src="https://img.shields.io/badge/slack-4A154B?style=for-the-badge&logo=slack&logoColor=white"> <img src="https://img.shields.io/badge/notion-000000?style=for-the-badge&logo=notion&logoColor=white">

<br/>
<br/>

# 🔗 Flow
![image](https://github.com/COFFEE-BARA/crawler-kyobo-isbn/assets/65851554/3414671f-3a73-481e-8d06-bc84881d9bb8)


1. 일주일에 한번 kyobo_isbn_crawler 람다가 실행
2. DynamoDB의 kyobo_cate_code_time 테이블에서 최근 크롤링시간과 카테고리별 최신 출간일을 가져와 이후에 출간된 책의 isbn 값을 크롤링
3. Elastic Search의 kyobo_isbn_cate 인덱스에 isbn값과 크롤링시간을 저장하고, 크롤링 자체 정보들을 DynamoDB의 kyobo_cate_code_time 테이블에 저장
4. DynamoDB 변경사항이 트리거인 naver_book_crawler 람다가 실행
5. naver_book_crawler 람다는 kyobo_isbn_cate 인덱스에서 값을 가져와 NABER OPEN API api를 통해 책정보를 구성
6. LLM으로 데이터를 벡터 변환하는 파이프라인인 vectorize_books_all을 거쳐 최종적으로 북 인덱스에 책정보를 적재
