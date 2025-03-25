#!/usr/bin/env bash

# helper script that runs the build script inside a gradle based docker container

source .env

echo "GRADLE_VERSION ${GRADLE_VERSION}"

docker run --rm \
  -v .:/opt/gen \
  -w /opt/gen \
  -u gradle \
  gradle:${GRADLE_VERSION} ./gradlew build
