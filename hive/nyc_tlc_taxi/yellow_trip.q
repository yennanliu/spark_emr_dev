CREATE EXTERNAL TABLE `yellow_trip`(
  `vendor_name` string, 
  `trip_pickup_datetime` timestamp, 
  `trip_dropoff_datetime` timestamp, 
  `passenger_count` integer, 
  `trip_distance` double, 
  `start_lon` double, 
  `start_lat` double, 
  `rate_code` string, 
  `store_and_forward` string,
  `end_lon` double,
  `end_lat` double, 
  `payment_type` string,
  `fare_amt` string,
  `surcharge` double,
  `mta_tax` double,
  `tip_amt` double,
  `tolls_amt` double,
  `total_amt` double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS
INPUTFORMAT
  'com.amazonaws.emr.s3select.hive.S3SelectableTextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://nyc-tlc-taxi/yellow_trip/'
TBLPROPERTIES (
  "s3select.format" = "csv",
  "s3select.headerInfo" = "ignore"
);