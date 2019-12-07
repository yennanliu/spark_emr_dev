CREATE EXTERNAL TABLE IF NOT EXISTS sparkify_song.artists_table (
  `artist_name` string,
  `artist_location` string,
  `artist_latitude` DOUBLE,
  `artist_longitude` DOUBLE 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://sparkify-song-data/etl_output/artists_table/'
TBLPROPERTIES ('has_encrypted_data'='false');