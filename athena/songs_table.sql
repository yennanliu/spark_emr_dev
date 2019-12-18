CREATE EXTERNAL TABLE IF NOT EXISTS sparkify_song.songs_table (
  `song_id` string,
  `num_songs` integer,
  `title` string,
  `artist_name` string,
  `artist_latitude` double,
  `year` integer,
  `duration` double,
  `artist_id` string,
  `artist_longitude` double,
  `artist_location` string 
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://sparkify-song-data/song-data/'
TBLPROPERTIES ('has_encrypted_data'='false');