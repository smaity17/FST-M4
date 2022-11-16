--Load the input
Tables = LOAD 'hdfs:///user/Soumen' USING PigStorage('\t') AS(r_Per:chararray,Lines:chararray);
--Group data using Character
Groupd = GROUP Tables BY r_Per;
--Generate Result Format
Counts = FOREACH Groupd GENERATE $0,COUNT($1) AS numlines;
--Orderby
Orders = ORDER Counts BY numlines DESC;
--Store
STORE Orders INTO 'hdfs:///user/Soumen/results';