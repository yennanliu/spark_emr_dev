#!/bin/sh
#################################################################
# HELP SCRIPT PARTITION S3 FILES 
#################################################################

copy_s3_file_from_bucket_to_bucket() {

aws s3  cp --recursive "s3://nyc-tlc-taxi/trip data/" "s3://nyc-tlc-taxi/trip_data/"
}

partition_s3_file() { 

from_bucket=s3://nyc-tlc-taxi/trip_data
to_bucket=s3://nyc-tlc-taxi/trip_data/yellow_trip
#s3_files=`aws s3 ls s3://nyc-tlc-taxi/trip_data/ | cut -d " " -f 4`
#s3_files=`aws s3 ls s3://nyc-tlc-taxi/trip_data/ | cut -d " " -f 3`
s3_files=`aws s3 ls s3://nyc-tlc-taxi/trip_data/`


for s3_file in $s3_files;
    do
    if [[ $s3_file =~ ^yellow_tripdata ]] 
       then   echo $s3_file
       #to_file=$(echo $s3_file | cut -d " "  -f 4 | cut -d "_" -f 2 | cut -d "." -f 1)
       #echo "copy" $from_bucket/$s3_file "to" $to_bucket/dt=$to_file/$s3_file 
       #aws s3 cp $from_bucket/$s3_file $to_bucket/dt=$to_file/$s3_file
    elif [[ $s3_file =~ ^green_tripdata ]] 
        then   echo $s3_file

    elif [[ $s3_file =~ ^fhv_tripdata ]] 
        then   echo $s3_file
    else 
        continue 
    fi 
    done 
}

partition_s3_file