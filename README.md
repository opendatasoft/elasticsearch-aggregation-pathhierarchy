Elasticsearch Aggregation Envelope Plugin
=========================================

The envelope aggregation plugin adds the possibility to compute convex envelope for geo points.

This is a metric aggregation.

|   Envelope aggregation Plugin  | elasticsearch     | Release date |
|--------------------------------|-------------------|:------------:|
| 6.2.4                          | 6.2.4             |              |
| 1.2.0                          | 1.4.0 -> master   |  2014-11-27  |
| 1.1.0                          | 1.3.0             |  2014-07-25  |
| 1.0.0                          | 1.2.2             |  2014-07-16  |


Usage
-----

```json
{
  "aggregations": {
    "<aggregation_name>": {
      "envelope": {
        "field": "<field_name>"
      }
    }
  }
}
```

`field` must be of type geo_point.

It returns a Geometry:

- Point if the bucket contains only one unique point
- LineString if the bucket contains two unique points
- Polygon if the bucket contains more than three unique points

For example :

```json
{
  "type": "Polygon",
  "coordinates": [
         [
            [
               2.3561,
               48.8322
            ],
            [
               2.33,
               48.8493
            ],
            [
               2.3333,
               48.8667
            ],
            [
               2.3615,
               48.8637
            ],
            [
               2.3561,
               48.8322
            ]
         ]
    ]
}
```
Installation
------------

`bin/plugin --install envelope_aggregation --url "https://github.com/opendatasoft/elasticsearch-aggregation-envelope/releases/download/v1.2.0/elasticsearch-envelope-aggregation-1.2.0.zip"`

License
-------

This software is under The MIT License (MIT)