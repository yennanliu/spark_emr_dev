#!/bin/sh
#################################################################
# HELP SCRIPT COPY FILES FROM BUCKET TO BUCKET 
#################################################################

copy_s3_file_from_bucket_to_bucket() {

aws s3  cp --recursive "s3://nyc-tlc-taxi/trip data/" "s3://nyc-tlc-taxi/trip_data/"
}

copy_s3_file_from_bucket_to_bucket