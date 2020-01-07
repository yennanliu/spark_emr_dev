########################
# COPY DATA 
########################

# STEP 1) cp data from s3 to EMR HDFS
# bash
# hadoop distcp s3://hadoop-etl-dev/data /data
# STEP 2) check the result
# hadoop fs -cat /data/kv1.txt

########################
# LOAD DATA 
########################

# METHOD 1) load data from s3 to EMR HDFS
# pig
student = LOAD 's3://hadoop-etl-dev/data/student_data.txt' 
   USING PigStorage(',')
   as ( id:int, firstname:chararray, lastname:chararray, phone:chararray, 
   city:chararray );

B = LOAD 's3://hadoop-etl-dev/data/b.txt' using PigStorage(',') AS (a:int, b:int, c:int, d:int, e:int, f:int);

DUMP student;
DUMP B;

# METHOD 2) load data from EMR HDFS directly
# cp file from s3 to Hadoop HDFS first
# hadoop distcp  s3://hadoop-etl-dev/data /data  
student = LOAD '/data/student_data.txt' 
   USING PigStorage(',')
   as ( id:int, firstname:chararray, lastname:chararray, phone:chararray, 
   city:chararray );

B = LOAD '/data/b.txt' using PigStorage(',') AS (a:int, b:int, c:int, d:int, e:int, f:int);


########################
# STORE DATA 
########################

# METHOD 1) save to EMR HDFS
# pig
STORE student INTO '/pig_output/' USING PigStorage (',');

# METHOD 2) save to S3
STORE student INTO 's3://hadoop-etl-dev/pig_output/' USING PigStorage (',');


########################
# FILTER DATA 
########################

B_ = FILTER B BY (d == 8);
DUMP B_;
DESCRIBE B_;


########################
# FOR EACH
########################

A = LOAD '/data/a.txt' using PigStorage(',') AS (a:int, b:int, c:int, d:int, e:int, f:int);
A_ = FOREACH A GENERATE a, b;
DUMP A;

C = LOAD '/data/c.txt' using PigStorage(',') AS (a:int, b:int, c:int);
C_ = FOREACH C GENERATE a, b, c, 999;
DUMP C_;
DESCRIBE C_;

########################
# GROUP
########################

B = LOAD '/data/b.txt' using PigStorage(',') AS (a:int, b:int, c:int, d:int, e:int, f:int);
B_group = GROUP B BY a;
DUMP B_group;


########################
# COUNT
########################

C = LOAD '/data/c.txt' using PigStorage(',') AS (dates:chararray, catid:int, userid:chararray);

C_tmp_1 = GROUP C ALL;

C_tmp_2 = FOREACH C_tmp_1 GENERATE $1;

C_tmp_3 = FOREACH C_tmp_2 GENERATE COUNT(C);

DUMP C_tmp_2;
DUMP C_tmp_3;

########################
# TOTUPLE
########################

# TOTUPLE : TOTUPLE can transform Fields to Tuples

B = LOAD '/data/b.txt' using PigStorage(',') AS (a:int, b:int, c:int, d:int, e:int, f:int);

B_TOTUPLE = FOREACH B GENERATE TOTUPLE(a, b), c, d, TOTUPLE(e, f);

DUMP B_TOTUPLE;


########################
# FLATTEN
########################





