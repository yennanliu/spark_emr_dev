#!/bin/sh
#################################################################
# HELP SCRIPT COPY FILES FROM LOCAL TO BUCKET 
#################################################################

copy_data_to_hadoop_bucket() {

aws s3 cp --recursive "data"  "s3://hadoop-etl-dev/data"
}

copy_data_to_hadoop_bucket
