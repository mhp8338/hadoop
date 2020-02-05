/*
加载数据
LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE tablename [PARTITION (partcol1=val1, partcol2=val2 ...)]
LOCAL 本次系统
OVERWRITE 数据覆盖与追加
*/
LOAD DATA INPATH 'hdfs://hadoop000:8020/data/emp.txt' INTO TABLE emp;

-- INSERT OVERWRITE TABLE tablename1 [PARTITION (partcol1=val1, partcol2=val2 ...) [IF NOT EXISTS]] select_statement1 FROM from_statement;
CREATE TABLE emp1 AS SELECT * FROM emppp;
CREATE TABLE emp2 AS SELECT empo,ename,job,deptno FROM emppp;

INSERT OVERWRITE LOCAL DIRECTORY '/tmp/hive/'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT empo,ename,sal,deptno FROM emp1;

-- 更新建议采用NOSQL数据库，不建议insert


-- HIVE QL 基本统计
SELECT [ALL | DISTINCT] select_expr, select_expr, ...
  FROM table_reference
  [WHERE where_condition]
  [GROUP BY col_list]
  [ORDER BY col_list]
  [CLUSTER BY col_list
    | [DISTRIBUTE BY col_list] [SORT BY col_list]
  ]
 [LIMIT [offset,] rows]

SELECT * FROM emp1 WHERE deptno=10;
SELECT * FROM emp1 WHERE sal BETWEEN 800 AND 1500;
SELECT * FROM emp1 WHERE ename NOT IN ('SMITH','WARD');


-- HIVE QL聚合函数
SELECT COUNT(1) FROM emp1 WHERE deptno=10;
SELECT MAX(sal), MIN(sal), SUM(sal), AVG(sal) FROM emp1;

-- 分组函数
-- 出现在select中的字段，如果没有出现在聚合函数里，那么一定出现在gruop by里
-- 分组条件过滤使用having

SELECT deptno,AVG(sal) FROM emp1 GROUP BY deptno;
SELECT deptno,job,AVG(sal) FROM emp1 GROUP BY deptno,job;

SELECT deptno,AVG(sal) avg_sal FROM emp1
GROUP BY deptno
HAVING avg_sal>2000;

-- join使用
CREATE TABLE dept(
deptno INT,
dname STRING,
loc STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/home/hadoop/data/dept.txt'
OVERWRITE INTO TABLE dept;

SELECT
e.empo,e.ename,e.sal,e.deptno,d.dname
FROM emp1 e JOIN dept d ON e.deptno=d.deptno;

-- HIVE SQL 的执行计划
EXPLAIN EXTENDED
SELECT
e.empo,e.ename,e.sal,e.deptno,d.dname
FROM emp1 e JOIN dept d ON e.deptno=d.deptno;



