Elasticsearch Aggregation Path Hierarchy Plugin
=========================================

This plugin adds the possibility to create hierarchical aggregations.
Each term is split on a provided separator (default "/") then aggregated by level.
For a complete example see https://github.com/elastic/elasticsearch/issues/8896

Two different aggregations are available:
 - `path_hierarchy` for hierarchical aggregations on `keywords` field or `scripts`
 - `date_hierarchy` for hierachical aggregations on `date` fields. It is more optimized to use this aggregation for date instead of a script.

This is a multi bucket aggregation.


Installation
------------

`bin/plugin --install path_hierarchy --url "https://github.com/opendatasoft/elasticsearch-aggregation-pathhierarchy/releases/download/v7.17.6.1/pathhierarchy-aggregation-7.17.6.1.zip"`

Build
-----

Development Environment Setup
------------

Build the plugin using gradle:

`./gradlew build`

OR

`./gradlew assemble` (to avoid tests)

Then the following command will start a dockerized ES and will install the previously built plugin:

`
docker-compose up
`


> NOTE: In `docker-compose.yml` you can uncomment the debug env and attach a REMOTE JVM on `*:5005` to debug the plugin.

Path hierarchy aggregation
--------------------------

### Parameters

 - `field` or `script` : field to aggregate on
 - `separator` : separator for path hierarchy (default to "/")
 - `order` : order parameter to define how to sort result. Allowed parameters are `_key`, `_count` or sub aggregation name. Default to {"_count": "desc}.
 - `size`: size parameter to define how many buckets should be returned. Default to 10.
 - `shard_size`: how many buckets returned by each shards. Set to size if smaller, default to size if the search request needs to go to a single shard, and (size * 1.5 + 10) otherwise (more information here: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html#_shard_size_3).
 - `min_depth`: Set minimum depth level. Default to 0.
 - `max_depth`: Set maximum depth level. `-1` means no limit. Default to 3.
 - `depth`: Retrieve values for specified depth. Shortcut, instead of setting `min_depth` and `max_depth` parameters to the same value.
 - `keep_blank_path`: Keep blank path as bucket. if this option is set to false, chained separator will be ignored. Default to false.
 - `min_doc_count`: Return buckets containing at least `min_doc_count` document. Default to 0


Examples
-------

#### String field

```
# Add data:
PUT filesystem
{
  "mappings": {
    "properties": {
      "path": {
        "type": "keyword"
      }
    }
  }
}
POST filesystem/_bulk?refresh
{"index":{}}
{"path":"/My documents/Spreadsheets/Budget_2013.xls","views":10}
{"index":{}}
{"path":"/My documents/Spreadsheets/Budget_2014.xls","views":7}
{"index":{}}
{"path":"/My documents/Test.txt","views":1}


# Path hierarchy request :
GET /filesystem/_search?size=0
{
  "aggs": {
    "tree": {
      "path_hierarchy": {
        "field": "path",
        "separator": "/"
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
     "sum_other_doc_count": 0,
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
               "key": "Spreadsheets",
               "doc_count": 2,
               "total_views": {
                 "value": 17
               },
               "tree": {
                 "buckets": [
                   {
                     "key": "Budget_2013.xls",
                     "doc_count": 1,
                     "total_views": {
                       "value": 10
                     }
                   },
                   {
                     "key": "Budget_2014.xls",
                     "doc_count": 1,
                     "total_views": {
                       "value": 7
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
```

#### Script

```
PUT calendar
{
  "mappings": {
    "properties": {
      "date": {
        "type": "date"
      }
    }
  }
}

POST calendar/_bulk?refresh
{"index":{}}
{"date": "2012-01-10T02:47:28"}
{"index":{}}
{"date": "2012-01-05T01:43:35"}
{"index":{}}
{"date": "2012-05-01T12:24:19"}

GET /calendar/_search?size=0
{
  "aggs": {
    "tree": {
      "path_hierarchy": {
        "script": "doc['date'].value.toOffsetDateTime().format(DateTimeFormatter.ofPattern('yyyy/MM/dd'))",
        "order": {
          "_key": "asc"
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

Date hierarchy
--------------

### Parameters

 - `field` : field to aggregate on. This parameter is mandatory
 - `interval`: date interval used to create the hierarchy. Allowed values are: `years`, `months`, `days`, `hours`, `minutes`, `seconds` Default to `years`.
 - `order` : order parameter to define how to sort result. Allowed parameters are `_key`, `_count` or sub aggregation name. Default to {"_count": "desc}.
 - `size`: size parameter to define how many buckets should be returned. Default to 10.
 - `shard_size`: how many buckets returned by each shards. Set to size if smaller, default to size if the search request needs to go to a single shard, and (size * 1.5 + 10) otherwise (more information here: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html#_shard_size_3).
 - `min_doc_count`: Return buckets containing at least `min_doc_count` document. Default to 0


Example
-------

```

PUT calendar
{
  "mappings": {
    "properties": {
      "date": {
        "type": "date"
      }
    }
  }
}

PUT /calendar/_doc/1
{
  "date": "2012-01-10T02:47:28"
}
PUT /calendar/_doc/2
{
  "date": "2012-01-05T01:43:35"
}
PUT /calendar/_doc/3
{
  "date": "2012-05-01T12:24:19"
}

GET /calendar/_search?size=0
{
  "aggs": {
    "tree": {
      "date_hierarchy": {
        "field": "date",
        "interval": "days",
        "order": {
          "_key": "asc"
        }
      }
    }
  }
}

```

Installation
------------

Built with Java 17.

The first 3 digits of plugin version is Elasticsearch versioning. The last digit is used for plugin versioning under an elasticsearch version.

To install it, launch this command in Elasticsearch directory replacing the url with a release suiting your case (please check available releases [here](https://github.com/opendatasoft/elasticsearch-aggregation-pathhierarchy/releases)):
`./bin/elasticsearch-plugin install https://github.com/opendatasoft/elasticsearch-aggregation-pathhierarchy/releases/download/v7.17.6.1/pathhierarchy-aggregation-7.17.6.1.zip`


License
-------

This software is under The MIT License (MIT).
