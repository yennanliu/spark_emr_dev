CREATE EXTERNAL TABLE IF NOT EXISTS sparkify_song.processed_songs_table (
  `song_id` string,
  `title` string,
  `artist_id` string,
  `year` string,
  `duration` DOUBLE

)
-- PARTITIONED BY (year string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://sparkify-song-data/etl_output/songs_table/'
TBLPROPERTIES ('has_encrypted_data'='false');
