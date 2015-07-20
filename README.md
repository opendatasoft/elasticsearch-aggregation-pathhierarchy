Elasticsearch Aggregation Path Hierarchy Plugin
=========================================

This plugins adds the possibility to create hierarchical aggregations.
Each term is split on a provided separator (default "/") then aggregated by level.
For a complete example see https://github.com/elastic/elasticsearch/issues/8896

This is a multi bucket aggregation.


Installation
------------

`bin/plugin --install path_hierarchy --url "https://github.com/opendatasoft/elasticsearch-aggregation-pathhierarchy/releases/download/v1.6.0.2/elasticsearch-aggregation-pathhierarchy-1.6.0.2.zip"`


Usage
-----

### Parameters

 - `field` or `script` : field to aggregate on
 - `separator` : separator for path hierarchy (default to "/")
 - `order` : order parameter to define how to sort result. Allowed parameters are `_term`, `_count` or sub aggregation name. Default to {"_count": "desc}.
 - `max_depth`: Set maximum depth level. `-1` means no limit. Default to 3.


Examples
-------

#### String field

```
Add data:

PUT /filesystem
{
  "mappings": {
    "file": {
      "properties": {
        "path": {
          "type": "string",
          "index": "not_analyzed",
          "doc_values": true
        }
      }
    }
  }
}

PUT /filesystem/file/1
{
  "path": "/My documents/Spreadsheets/Budget_2013.xls",
  "views": 10
}

PUT /filesystem/file/2
{
  "path": "/My documents/Spreadsheets/Budget_2014.xls",
  "views": 7
}

PUT /filesystem/file/3
{
  "path": "/My documents/Test.txt",
  "views": 1
}



Path hierarchy request :

GET /filesystem/file/_search?search_type=count
{
  "aggs": {
    "tree": {
      "path_hierarchy": {
        "field": "path",
        "separator": "/",
        "order": {"_term": "desc"}
      },
      "aggs": {
        "total_views": {
          "sum": {
            "field": "views"
          }
        }
      }
    }
  }
}


Result :

{"aggregations": {
      "tree": {
         "buckets": [
            {
               "key": "My documents",
               "doc_count": 3,
               "total_views": {
                  "value": 18
               },
               "tree": {
                  "buckets": [
                     {
                        "key": "Test.txt",
                        "doc_count": 1,
                        "total_views": {
                           "value": 1
                        }
                     },
                     {
                        "key": "Spreadsheets",
                        "doc_count": 2,
                        "total_views": {
                           "value": 17
                        },
                        "tree": {
                           "buckets": [
                              {
                                 "key": "Budget_2014.xls",
                                 "doc_count": 1,
                                 "total_views": {
                                    "value": 7
                                 }
                              },
                              {
                                 "key": "Budget_2013.xls",
                                 "doc_count": 1,
                                 "total_views": {
                                    "value": 10
                                 }
                              }
                           ]
                        }
                     }
                  ]
               }
            }
         ]
      }
   }
}

```

#### Script

```

PUT calendar
{
  "mappings": {
    "date": {
      "properties": {
        "date": {
          "type": "date",
          "index": "not_analyzed",
          "doc_values": true
        }
      }
    }
  }
}

PUT /calendar/date/1
{
  "date": "2012-01-10T02:47:28"
}

PUT /calendar/date/2
{
  "date": "2012-01-05T01:43:35"
}

PUT /calendar/date/3
{
  "date": "2012-05-01T12:24:19"
}

GET /calendar/date/_search?search_type=count
{
  "aggs": {
    "tree": {
      "path_hierarchy": {
        "script": "doc['date'].date.toString('YYYY/MM/dd')",
        "order": {"_term": "asc"}
      }
    }
  }
}


Result :

{
   "aggregations": {
      "tree": {
         "buckets": [
            {
               "key": "2012",
               "doc_count": 3,
               "tree": {
                  "buckets": [
                     {
                        "key": "01",
                        "doc_count": 2,
                        "tree": {
                           "buckets": [
                              {
                                 "key": "05",
                                 "doc_count": 1
                              },
                              {
                                 "key": "10",
                                 "doc_count": 1
                              }
                           ]
                        }
                     },
                     {
                        "key": "05",
                        "doc_count": 1,
                        "tree": {
                           "buckets": [
                              {
                                 "key": "01",
                                 "doc_count": 1
                              }
                           ]
                        }
                     }
                  ]
               }
            }
         ]
      }
   }
}



```



License
-------

This software is under The MIT License (MIT)
