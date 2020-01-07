
A = LOAD '/data/a.txt' using PigStorage(',') AS (a:int, b:int, c:int, d:int, e:int, f:int);
A_ = FOREACH A GENERATE a, b;
DUMP A;

C = LOAD '/data/c.txt' using PigStorage(',') AS (a:int, b:int, c:int);
C_ = FOREACH C GENERATE a, b, c, 999;
DUMP C_;
DESCRIBE C_;