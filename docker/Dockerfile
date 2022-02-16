FROM docker.elastic.co/elasticsearch/elasticsearch:7.17.0 AS elasticsearch-plugin-debug

COPY /build/distributions/pathhierarchy-aggregation-7.17.0.0.zip /tmp/pathhierarchy-aggregation-7.17.0.0.zip
RUN ./bin/elasticsearch-plugin install file:/tmp/pathhierarchy-aggregation-7.17.0.0.zip