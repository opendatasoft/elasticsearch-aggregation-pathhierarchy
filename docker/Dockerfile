FROM docker.elastic.co/elasticsearch/elasticsearch:8.15.2 AS elasticsearch-plugin-debug

COPY /build/distributions/pathhierarchy-aggregation-8.15.2.1.zip /tmp/pathhierarchy-aggregation-8.15.2.1.zip
RUN ./bin/elasticsearch-plugin install file:/tmp/pathhierarchy-aggregation-8.15.2.1.zip
