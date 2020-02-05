-- 外部表的使用
/*
MANAGED_TABLE:
    - 删除表：hdfs被删除
    - meta信息也被删除
*/
CREATE EXTERNAL TABLE emp_external
(
    empno    INT,
    ename    STRING,
    job      STRING,
    mgr      INT,
    hiredate STRING,
    sal      DOUBLE,
    comm     DOUBLE,
    deptno   INT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/external/emp';

LOAD DATA LOCAL INPATH '/home/hadoop/data/emp.txt' OVERWRITE INTO TABLE emp_external;

/*EXTERNAL_TABLE:
    - 删除表：hdfs不被删除
    - meta信息被删除
    - 删除后重新创建表数据可恢复*/


-- ETL 加载至数据仓库
CREATE EXTERNAL TABLE track_info
(
    ip       STRING,
    country  STRING,
    province STRING,
    city     STRING,
    url      STRING,
    time     STRING,
    page     STRING
) PARTITIONED BY (day STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/project/trackinfo/';
-- 加载数据,加载完etl数据将被清除
LOAD DATA INPATH 'hdfs://hadoop000:8020/project/input/etl' OVERWRITE INTO TABLE track_info PARTITION (day = '2013-7-21');

SELECT COUNT(*)
FROM track_info
WHERE day = '2013-07-21';
-- crontab表达式进行调度
-- azkaban调度

SELECT province, COUNT(*)
FROM track_info
GROUP BY province;

-- HIVE统计分析
-- 省份统计表
CREATE EXTERNAL TABLE track_info_province
(
    province STRING,
    cnt      BIGINT
) PARTITIONED BY (day STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/project/trackinfo/province';
-- 导数据
INSERT OVERWRITE TABLE track_info_province PARTITION (day = '2013-07-21')
SELECT province, COUNT(*) AS cnt
FROM track_info
GROUP BY province;
/*
1) etl （spark mr）
2) 把etl数据加载至track_info分区表中
3) 各个维度的统计结果输入至各自维度中track_info_province
4) 将数据导出至RDBMS
*/
-- 将表的数据导出至RDBMS（sqoop）

-- MR 和 HIVE对比