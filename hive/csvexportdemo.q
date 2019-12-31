CREATE TABLE csvexportdemo (
  id BIGINT, time STRING, log STRING
  ) 
 row format delimited fields terminated by ',' 
 lines terminated by '\n' 
 STORED AS TEXTFILE
 LOCATION 's3n://nyc-tlc-taxi/hive_outout/csvexportdemo/csvexportdemo.csv';