# bash
# STEP 1) cp data from s3 to EMR HDFS
# hadoop distcp  s3://hadoop-etl-dev/data /data
# STEP 2) check the result
# hdfs dfs -cat hdfs://localhost:9000/student_data.txt

# pig
# METHOD 1) load data from s3 to EMR HDFS
student = LOAD 's3://hadoop-etl-dev/data/student_data.txt' 
   USING PigStorage(',')
   as ( id:int, firstname:chararray, lastname:chararray, phone:chararray, 
   city:chararray );

# METHOD 2) load data form EMR HDFS directly 