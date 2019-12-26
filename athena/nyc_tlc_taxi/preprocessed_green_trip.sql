CREATE EXTERNAL TABLE IF NOT EXISTS nyc_taxi_tlc.preprocessed_green_trip (
  `taxi_type` string,
  `vendor_id` INT,
  `pickup_datetime` bigint,
  `dropoff_datetime` bigint,
  `store_and_fwd_flag` string,
  `rate_code_id` INT,
  `pickup_location_id` INT,
  `dropoff_location_id` INT,
  `pickup_longitude` BINARY,
  `pickup_latitude` BINARY,
  `dropoff_longitude` BINARY,
  `dropoff_latitude` BINARY,
  `passenger_count` INT,
  `trip_distance` DOUBLE,
  `fare_amount` DOUBLE,
  `extra` DOUBLE,
  `mta_tax` DOUBLE,
  `tip_amount` DOUBLE,
  `tolls_amount` DOUBLE,
  `improvement_surcharge` DOUBLE,
  `total_amount` DOUBLE,
  `payment_type` INT,
  `trip_year` string
) PARTITIONED BY (
  trip_month string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://nyc-tlc-taxi/scala_etl_output/PreProcessGreenTaxiTrip/trip_year=2016/'
TBLPROPERTIES ('has_encrypted_data'='false');