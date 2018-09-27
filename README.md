Elasticsearch Aggregation Path Hierarchy Plugin
=========================================

This plugins adds the possibility to create hierarchical aggregations.
Each term is split on a provided separator (default "/") then aggregated by level.
For a complete example see https://github.com/elastic/elasticsearch/issues/8896

This is a multi bucket aggregation.


Installation
------------

`bin/plugin --install path_hierarchy --url "https://github.com/opendatasoft/elasticsearch-aggregation-pathhierarchy/releases/download/v6.4.1.0/elasticsearch-aggregation-pathhierarchy-6.4.1.0.zip"`


Usage
-----

### Parameters

 - `field` or `script` : field to aggregate on
 - `separator` : separator for path hierarchy (default to "/")
 - `order` : order parameter to define how to sort result. Allowed parameters are `_key`, `_count` or sub aggregation name. Default to {"_count": "desc}.
 - `size`: size parameter to define how many buckets should be returned. Default to 10.
 - `min_depth`: Set minimum depth level. Default to 0.
 - `max_depth`: Set maximum depth level. `-1` means no limit. Default to 3.
 - `depth`: Retrieve values for specified depth. Shortcut, instead of setting `min_depth` and `max_depth` parameters to the same value.


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
          "type": "keyword",
          "index": false,
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

PUT /filesystem/file/4
{
  "path": "/My documents/Spreadsheets/Budget_2014.xls",
  "views": 12
}



Path hierarchy request :

GET /filesystem/file/_search?size=0
{
  "aggs": {
    "tree": {
      "path_hierarchy": {
        "field": "path",
        "separator": "/",
        "order": [{"_count": "desc"}, {"_key": "asc"}],
        "minDepth": 0,
        "maxDepth": 3
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
          "doc_count": 4,
          "total_views": {
            "value": 30
          },
          "tree": {
            "buckets": [
              {
                "key": "Spreadsheets",
                "doc_count": 3,
                "total_views": {
                  "value": 29
                },
                "tree": {
                  "buckets": [
                    {
                      "key": "Budget_2014.xls",
                      "doc_count": 2,
                      "total_views": {
                        "value": 19
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
              },
              {
                "key": "Test.txt",
                "doc_count": 1,
                "total_views": {
                  "value": 1
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
          "index": false,
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

GET /calendar/date/_search?size=0
{
  "aggs": {
    "tree": {
      "path_hierarchy": {
        "script": "doc['date'].value.toString('YYYY/MM/dd')",
        "order": {"_key": "asc"}
      }
    }
  }
}


Result :

{"aggregations": {
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


Installation
------------

Plugin versions are available for (at least) all minor versions of Elasticsearch since 6.0.

The first 3 digits of plugin version is Elasticsearch versioning. The last digit is used for plugin versioning under an elasticsearch version.

To install it, launch this command in Elasticsearch directory replacing the url by the correct link for your Elasticsearch version (see table)
`./bin/elasticsearch-plugin install https://github.com/opendatasoft/elasticsearch-aggregation-pathhierarchy/releases/download/v6.4.1.0/elasticsearch-aggregation-pathhierarchy-6.4.1.0.zip`

| elasticsearch version | plugin version | plugin url |
| --------------------- | -------------- | ---------- |
| 1.6.0 | 1.6.0.4 | https://github.com/opendatasoft/elasticsearch-aggregation-pathhierarchy/releases/download/v1.6.0.4/elasticsearch-aggregation-pathhierarchy-1.6.0.4.zip |
| 6.0.1 | 6.0.1.0 | https://github.com/opendatasoft/elasticsearch-aggregation-pathhierarchy/releases/download/v6.0.1.0/elasticsearch-aggregation-pathhierarchy-6.0.1.0.zip |
| 6.1.4 | 6.1.4.0 | https://github.com/opendatasoft/elasticsearch-aggregation-pathhierarchy/releases/download/v6.1.4.0/elasticsearch-aggregation-pathhierarchy-6.1.4.0.zip |
| 6.2.4 | 6.2.4.0 | https://github.com/opendatasoft/elasticsearch-aggregation-pathhierarchy/releases/download/v6.2.4.0/elasticsearch-aggregation-pathhierarchy-6.2.4.0.zip |
| 6.3.2 | 6.3.2.0 | https://github.com/opendatasoft/elasticsearch-aggregation-pathhierarchy/releases/download/v6.3.2.0/elasticsearch-aggregation-pathhierarchy-6.3.2.0.zip |
| 6.4.1 | 6.4.1.0 | https://github.com/opendatasoft/elasticsearch-aggregation-pathhierarchy/releases/download/v6.4.1.0/elasticsearch-aggregation-pathhierarchy-6.4.1.0.zip |


License
-------

This software is under The MIT License (MIT).
