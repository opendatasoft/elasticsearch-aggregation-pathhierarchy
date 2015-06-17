Elasticsearch Aggregation Path Hierarchy Plugin
=========================================

This is a multi bucket aggregation.


Installation
------------

`bin/plugin --install path_hierarchy --url "https://github.com/opendatasoft/elasticsearch-aggregation-pathhierarchy/releases/download/v1.6.0.0/elasticsearch-aggregation-pathhierarchy-1.6.0.0.zip"`


Usage
-----

### Parameters

 - `field` or `script` : field to aggregate on
 - `separator` : separator for path hierarchy (default to "/")
 - `order` : order parameter to define how to sort result. Allowed parameters are `_term`, `_count` or sub aggregation name. Default to {"_count": "desc}.
 - `max_depth`: Set maximum depth level. `-1` means no limit. Default to 3.


For a complete example see https://github.com/elastic/elasticsearch/issues/8896

License
-------

This software is under The MIT License (MIT)
