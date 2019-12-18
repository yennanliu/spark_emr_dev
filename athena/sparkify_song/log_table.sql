CREATE EXTERNAL TABLE IF NOT EXISTS sparkify_song.log_table (
  `artist` string,
  `auth` string,
  `firstName` string,
  `gender` string,
  `itemInSession` string,
  `lastName` string,
  `length` float,
  `level` string,
  `location` string,
  `method` string,
  `page` string,
  `registration` float,
  `sessionId` integer,
  `song` string,
  `status` string,
  `ts` timestamp,
  `userAgent` string,
  `userId` string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://sparkify-song-data/log-data/'
TBLPROPERTIES ('has_encrypted_data'='false');