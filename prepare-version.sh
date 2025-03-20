#!/usr/bin/env bash

# Helper script to prepare a version of the plugin based on a specific elasticsearch version
#
# given the elasticsearch version as argument, it generates the needed configuration file to build the plugin

[ -z "$1" ] && {
    echo "1st argument should be the targeted elasticsearch version"
    exit 1
}

ES_VERSION="$1"

# retrieve version information from ES repository
GRADLE_VERSION=$(curl -s https://raw.githubusercontent.com/elastic/elasticsearch/refs/tags/v${ES_VERSION}/build-tools-internal/src/main/resources/minimumGradleVersion)
JAVA_COMPILER_VERSION=$(curl -s https://raw.githubusercontent.com/elastic/elasticsearch/refs/tags/v${ES_VERSION}/build-tools-internal/src/main/resources/minimumCompilerVersion)
JAVA_RUNTIME_VERSION=$(curl -s https://raw.githubusercontent.com/elastic/elasticsearch/refs/tags/v${ES_VERSION}/build-tools-internal/src/main/resources/minimumRuntimeVersion)

PLUGIN_VERSION="${ES_VERSION}.0"

echo "GRADLE_VERSION ${GRADLE_VERSION}"
echo "JAVA_COMPILER_VERSION ${JAVA_COMPILER_VERSION}"
echo "ES_VERSION ${ES_VERSION}"
echo "PLUGIN_VERSION ${PLUGIN_VERSION}"

echo "es_version = ${ES_VERSION}
plugin_version = ${PLUGIN_VERSION}" > gradle.properties

echo "ES_VERSION=${ES_VERSION}
PLUGIN_VERSION=${PLUGIN_VERSION}
JAVA_COMPILER_VERSION=${JAVA_COMPILER_VERSION}
GRADLE_VERSION=${GRADLE_VERSION}" > .env

docker run --rm \
  -v .:/opt/gen \
  -w /opt/gen \
  -u gradle \
  gradle:"${GRADLE_VERSION}" /usr/bin/gradle wrapper --gradle-version "${GRADLE_VERSION}" --distribution-type bin
