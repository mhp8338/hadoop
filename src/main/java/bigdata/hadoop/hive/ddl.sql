-- 删除数据库
-- DROP (DATABASE|SCHEMA) [IF EXISTS] database_name [RESTRICT|CASCADE];
DROP DATABASE IF EXISTS test;
DROP DATABASE test CASCADE;

-- 表的ddl操作
CREATE [TEMPORARY] [EXTERNAL] TABLE [IF NOT EXISTS] [db_name.]table_name    -- (Note: TEMPORARY available in Hive 0.14.0 and later)
  [(col_name data_type [column_constraint_specification] [COMMENT col_comment], ... [constraint_specification])]
  [COMMENT table_comment]
  [PARTITIONED BY (col_name data_type [COMMENT col_comment], ...)]
  [CLUSTERED BY (col_name, col_name, ...) [SORTED BY (col_name [ASC|DESC], ...)] INTO num_buckets BUCKETS]
  [SKEWED BY (col_name, col_name, ...)                  -- (Note: Available in Hive 0.10.0 and later)]
     ON ((col_value, col_value, ...), (col_value, col_value, ...), ...)
     [STORED AS DIRECTORIES]
  [
   [ROW FORMAT row_format]
   [STORED AS file_format]
     | STORED BY 'storage.handler.class.name' [WITH SERDEPROPERTIES (...)]  -- (Note: Available in Hive 0.6.0 and later)
  ]
  [LOCATION hdfs_path]
  [TBLPROPERTIES (property_name=property_value, ...)]   -- (Note: Available in Hive 0.6.0 and later)
  [AS select_statement];   -- (Note: Available in Hive 0.5.0 and later; not supported for external tables)

CREATE TABLE emp(
empo INT ,
ename STRING,
job STRING,
mgr INT,
hiredate STRING,
sal DOUBLE ,
comm DOUBLE ,
deptno INT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

DESC FORMATTED emp;

-- 导入数据
LOAD DATA LOCAL INPATH '/home/hadoop/data/emp.txt' OVERWRITE INTO TABLE emp;

-- 改名字
-- ALTER TABLE table_name RENAME TO new_table_name;
ALTER TABLE emp RENAME TO emppp;