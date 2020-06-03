#!/usr/bin/env bash
set -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "====Building Producer JARs===="
$DIR/../CitibikeApiProducer/gradlew -p $DIR/../CitibikeApiProducer test
echo "====Building Consumer JARs===="
cd $DIR/../RawDataSaver && sbt test
cd $DIR/../StationConsumer && sbt test
cd $DIR/../StationTransformerNYC && sbt test