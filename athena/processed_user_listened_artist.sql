CREATE EXTERNAL TABLE IF NOT EXISTS sparkify_song.processed_user_listened_artist (
  `userid` string,
  `artist_array` array<string> 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://sparkify-song-data/song-data/scala_etl_output/get_user_listened_song/'
TBLPROPERTIES ('has_encrypted_data'='false');