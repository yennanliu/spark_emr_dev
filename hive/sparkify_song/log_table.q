CREATE EXTERNAL TABLE IF NOT EXISTS log_table (
  artist string,
  auth string,
  firstName string,
  gender string,
  itemInSession string,
  lastName string,
  length float,
  level string,
  location string,
  method string,
  page string,
  registration float,
  sessionId integer,
  song string,
  status string,
  ts timestamp,
  userAgent string,
  userId string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
LOCATION 's3://sparkify-song-data/log-data/2018/11/';
-- Total requests per operating system for a given time frame
INSERT OVERWRITE DIRECTORY 's3://sparkify-song-data/hive_etl_output/' SELECT * FROM log_table;