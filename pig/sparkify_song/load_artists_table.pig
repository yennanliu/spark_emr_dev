
#hadoop distcp s3://sparkify-song-data/etl_output/artists_table /sparkify-song-data/etl_output/artists_table

artists_table= LOAD '/sparkify-song-data/etl_output/artists_table' using PigStorage(',') AS (artist_name: int,  artist_location: int, artist_latitude: int,  artist_longitude: int);

DUMP artists_table;

