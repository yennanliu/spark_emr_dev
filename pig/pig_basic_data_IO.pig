########################
# COPY DATA 
########################

# bash
# STEP 1) cp data from s3 to EMR HDFS
# hadoop distcp  s3://hadoop-etl-dev/data /data
# STEP 2) check the result
# hdfs dfs -cat hdfs://localhost:9000/student_data.txt

########################
# LOAD DATA 
########################

# pig
# METHOD 1) load data from s3 to EMR HDFS

student = LOAD 's3://hadoop-etl-dev/data/student_data.txt' 
   USING PigStorage(',')
   as ( id:int, firstname:chararray, lastname:chararray, phone:chararray, 
   city:chararray );

B = LOAD 's3://hadoop-etl-dev/data/b.txt' using PigStorage(',') AS (a:int, b:int, c:int, d:int, e:int, f:int);

DUMP student;
DUMP B;

# METHOD 2) load data form EMR HDFS directly 


########################
# FILTER DATA 
########################

B_ = FILTER B BY (d == 8);
DUMP B_;
DESCRIBE B_;





