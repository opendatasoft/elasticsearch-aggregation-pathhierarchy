ARG ES_VERSION
ARG PLUGIN_VERSION

FROM docker.elastic.co/elasticsearch/elasticsearch:${ES_VERSION} AS elasticsearch-plugin-debug

COPY /build/distributions/pathhierarchy-aggregation-${PLUGIN_VERSION}.zip /tmp/pathhierarchy-aggregation-${PLUGIN_VERSION}.zip
RUN ./bin/elasticsearch-plugin install file:/tmp/pathhierarchy-aggregation-${PLUGIN_VERSION}.zip