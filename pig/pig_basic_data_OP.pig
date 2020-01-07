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

B = LOAD '/data/b.txt' using PigStorage(',') AS (a:int, b:int, c:int, d:int, e:int, f:int);

B_tuple = FOREACH B GENERATE TOTUPLE(a, b), c, d, TOTUPLE(e, f);

B_tuple_foreach = FOREACH B_tuple GENERATE FLATTEN($0), c, d, FLATTEN($3);

DUMP B_tuple;

DUMP B_tuple_foreach;

########################
# ORDER
########################

B = LOAD '/data/b.txt' using PigStorage(',') AS (a:int, b:int, c:int, d:int, e:int, f:int);

B_order = ORDER B BY a ASC, d DESC;

DUMP B_order;

########################
# UNION
########################

A = LOAD '/data/a.txt' using PigStorage(',') AS (a:int, b:int, c:int);

B = LOAD '/data/b.txt' using PigStorage(',') AS (a:int, b:int, c:int, d:int, e:int, f:int);

A_B_union = UNION A, B;

DUMP A_B_union;

########################
# DISTINCT
########################

A = LOAD '/data/a.txt' using PigStorage(',') AS (a:int, b:int, c:int);

B = LOAD '/data/b.txt' using PigStorage(',') AS (a:int, b:int, c:int, d:int, e:int, f:int);

TMP_B = FOREACH B GENERATE a, b, c;

UNION_A_TMP_B = UNION A, TMP_B;

DISTINCT_UNION_A_TMP_B = DISTINCT UNION_A_TMP_B;

DUMP UNION_A_TMP_B;

DUMP DISTINCT_UNION_A_TMP_B;

########################
# CROSS
########################

A = LOAD '/data/a.txt' using PigStorage(',') AS (a:int, b:int, c:int);

B = LOAD '/data/b.txt' using PigStorage(',') AS (a:int, b:int, c:int, d:int, e:int, f:int);

D = CROSS B, A;

E = CROSS A, B;

DUMP D;

########################
# SUM
########################

D= LOAD '/data/d.txt' using PigStorage(',') AS (owner:chararray, pet_type:chararray, pet_num:int);

B = GROUP D BY owner;

C = FOREACH B GENERATE group, SUM(A.pet_num);

DUMP B;

DUMP C;

########################
# SIZE
########################

D = LOAD '/data/d.txt' using PigStorage(',') AS (owner:chararray, pet_type:chararray, pet_num:int);

B = FOREACH D GENERATE SIZE(pet_type);

C = FOREACH D GENERATE SIZE(pet_num);

DUMP B;
DUMP C;

########################
# MAX / MIN
########################

E = LOAD '/teaching/e.txt' using PigStorage(',') AS (name:chararray, session:chararray, gpa:float);

B = GROUP E BY name;

C = FOREACH B GENERATE group, MAX(A.gpa);

C_ = FOREACH B GENERATE group, MIN(A.gpa);

DUMP C;
DUMP C_;

########################
# TOBAG
########################

E = LOAD '/data/e.txt' using PigStorage(',') AS (name:chararray, session:chararray, gpa:float);

B = FOREACH E GENERATE TOBAG(name, gpa);

DUMP B;

########################
# DIFF
########################

F = LOAD '/data/f.txt' using PigStorage(',') as (a:int, b:int, c:int, d:int, e:int, f:int, g:int , h:int);

B = FOREACH F GENERATE TOTUPLE(a, b), TOTUPLE(c, d), TOTUPLE(e, f), TOTUPLE(g, h);

C = FOREACH B GENERATE TOBAG($0, $1), TOBAG($2, $3);

D = FOREACH C GENERATE DIFF($0,$1);

DUMP C;

DUMP D;
