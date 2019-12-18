CREATE EXTERNAL TABLE `yellow_trip`(
  `vendorid` string, 
  `tpep_pickup_datetime` timestamp, 
  `tpep_dropoff_datetime` timestamp, 
  `passenger_count` integer, 
  `trip_distance` double, 
  `ratecodeid` string, 
  `store_and_fwd_flag` string, 
  `pulocationid` string, 
  `dolocationid` string, 
  `payment_type` string, 
  `fare_amount` double, 
  `extra` double, 
  `mta_tax` string, 
  `tip_amount` double, 
  `tolls_amount` double, 
  `improvement_surcharge` string, 
  `total_amount` double, 
  `congestion_surcharge` double)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://nyc-tlc-taxi/yellow_trip/';