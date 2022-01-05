FROM docker.elastic.co/elasticsearch/elasticsearch:7.10.2 AS elasticsearch-plugin-debug

COPY /build/distributions/pathhierarchy-aggregation-7.10.2.zip /tmp/pathhierarchy-aggregation-7.10.2.zip
RUN ./bin/elasticsearch-plugin install file:/tmp/pathhierarchy-aggregation-7.10.2.zip