from ES_Manager import ESM
import json

def GET_SEARCH_RESULT(query, elastic_endpoint):
    object = ESM(elastic_endpoint)
    es = object.Check_ES_Conn()
    if es.error == None:
        search_result = es.Search_Result(query, index=None)
        return(search_result)
    else: print(es.error)



query = {
    "aggs": {
        "2": {
            "terms": {
                "field": "machine.os.keyword",
                "order": {
                    "_count": "asc" ## replaced with asc to get the expected output data in the PDF
                },
                "size": 10
            }
        }
    },
    "size": 0,
    "stored_fields": [
        "*"
    ],
    "script_fields": {
        "hour_of_day": {
            "script": {
                "source": "doc['timestamp'].value.getHour()",
                "lang": "painless"
            }
        }
    },
    "docvalue_fields": [
        {
            "field": "@timestamp",
            "format": "date_time"
        },
        {
            "field": "timestamp",
            "format": "date_time"
        },
        {
            "field": "utc_time",
            "format": "date_time"
        }
    ],
    "_source": {
        "excludes": []
    },
    "query": {
        "bool": {
            "must": [],
            "filter": [
                {
                    "match_all": {}
                },
                {
                    "range": {
                        "timestamp": {
                            "gte": "2021-07-02T04:06:52.020Z",
                            "lte": "2021-07-09T04:06:52.020Z",
                            "format": "strict_date_optional_time"
                        }
                    }
                }
            ],
            "should": [],
            "must_not": []
        }
    }
}


if __name__ == "__main__":
    x = GET_SEARCH_RESULT(query, elastic_endpoint='https://some-elastic-endpoint-they-gave-me-that-i-cant-show-you')
    result_list = x['aggregations']['2']['buckets']
    results_arr = [['KEYS', 'DOC_COUNTS']]
    total= x['hits']['total']['value']
    expected_output_data = [[],[]]
    for result in result_list:
        results_arr.append([result['key'], result['doc_count']])
        expected_output_data[0].append(result['key'])
        expected_output_data[1].append("{:.2%}".format(int(result['doc_count'])/total))

    result = json.dumps(results_arr)
    result_json = open("result.json", "w")
    result_json.write(result)
    result_json.close()

    expected_output_data = json.dumps(expected_output_data)
    expected_output_json = open("expected_output_data.json", "w")
    expected_output_json.write(expected_output_data)
    expected_output_json.close()
    print("Result JSON content:", result) ## this is the 
    print("Expected output data:", expected_output_data) ##this is the expected result in the PDF of the task
