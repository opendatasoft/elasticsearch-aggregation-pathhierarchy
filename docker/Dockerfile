ARG ES_VERSION
ARG PLUGIN_FILENAME

FROM docker.elastic.co/elasticsearch/elasticsearch:${ES_VERSION} AS elasticsearch-plugin-debug

COPY build/distributions/${PLUGIN_FILENAME} /tmp/${PLUGIN_FILENAME}

# mandatory because docker ARG is scoped to stage (forgotten after the FROM scope above...!)
ARG PLUGIN_FILENAME
# madantory because ARGS cannot be used inside RUN shell
ENV PLUGIN_FILENAME=${PLUGIN_FILENAME}
RUN ./bin/elasticsearch-plugin install --batch file:/tmp/${PLUGIN_FILENAME}
