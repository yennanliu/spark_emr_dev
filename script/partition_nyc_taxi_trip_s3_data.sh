#!/bin/sh
#################################################################
# HELP SCRIPT PARTITION S3 FILES 
#################################################################

partition_s3_file() { 

from_bucket=s3://nyc-tlc-taxi/trip_data
to_yellow_bucket=s3://nyc-tlc-taxi/trip_data/yellow_trip
to_green_bucket=s3://nyc-tlc-taxi/trip_data/green_trip
to_fhv_bucket=s3://nyc-tlc-taxi/trip_data/fhv_trip
s3_files=`aws s3 ls s3://nyc-tlc-taxi/trip_data/`

for s3_file in $s3_files;
    do
    if [[ $s3_file =~ ^yellow_tripdata ]] 
       then   
         echo $s3_file
         to_file=$(echo $s3_file | cut -d " "  -f 4 | cut -d "_" -f 3 | cut -d "." -f 1)
         echo "copy" $from_bucket/$s3_file "to" $to_yellow_bucket/dt=$to_file/$s3_file 
         #aws s3 cp $from_bucket/$s3_file $to_bucket/dt=$to_file/$s3_file
    
    elif [[ $s3_file =~ ^green_tripdata ]] 
        then  
          to_file=$(echo $s3_file | cut -d " "  -f 4 | cut -d "_" -f 3 | cut -d "." -f 1)
          echo "copy" $from_bucket/$s3_file "to" $to_green_bucket/dt=$to_file/$s3_file
          #aws s3 cp $from_bucket/$s3_file $to_bucket/dt=$to_file/$s3_file 

    elif [[ $s3_file =~ ^fhv_tripdata ]] 
        then  
          to_file=$(echo $s3_file | cut -d " "  -f 4 | cut -d "_" -f 3 | cut -d "." -f 1)
          echo "copy" $from_bucket/$s3_file "to" $to_fhv_bucket/dt=$to_file/$s3_file 
          #aws s3 cp $from_bucket/$s3_file $to_bucket/dt=$to_file/$s3_file
    else 
        continue 
    fi 
    done 
}

partition_s3_file