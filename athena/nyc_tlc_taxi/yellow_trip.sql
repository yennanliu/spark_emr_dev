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
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://nyc-tlc-taxi/yellow_trip/';
