CREATE EXTERNAL TABLE IF NOT EXISTS sparkify_song.log_table (
  `artist` string,
  `auth` string,
  `firstName` string,
  `gender` string,
  `itemInSession` string,
  `lastName` string,
  `length` string,
  `level` string,
  `location` string,
  `method` string,
  `page` string,
  `registration` string,
  `sessionId` string,
  `song` string,
  `status` string,
  `ts` string,
  `userAgent` string,
  `userId` string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://sparkify-song-data/log-data/'
TBLPROPERTIES ('has_encrypted_data'='false');