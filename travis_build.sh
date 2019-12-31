#!/bin/sh

echo '>>>> DOCKER BUILD...'
docker build . -t spark_env
echo '>>>> RUN SCALA SPARK ETL PIPELINE BUILD...'
docker run  --mount \
type=bind,\
source="$(pwd)"/.,\
target=/spark_emr_dev \
-i -t spark_env \
/bin/bash  -c "cd ../spark_emr_dev && sbt clean compile && sbt assembly && spark-submit /spark_emr_dev/target/scala-2.11/spark-etl-pipeline-assembly-1.0.jar"
