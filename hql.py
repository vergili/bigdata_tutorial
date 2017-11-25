HQL_HDFS_TO_HIVE_TRANSFER = """
    DROP TABLE IF EXISTS {table_name};

    CREATE EXTERNAL TABLE {tmp_table_name} (
        column1 STRING,
        column2 STRING,
        column3 STRING,
        column4 STRING,
        column5 STRING, 
        column6 STRING,
        column7 STRING, 
        column8 STRING, 
        column9 STRING, 
        column10 STRING,
        column11 STRING,
        column12 STRING,
        column13 STRING,
        column14 STRING,
        column15 STRING, 
        column16 STRING,
        column17 STRING, 
        column18 STRING, 
        column19 STRING, 
        column20 STRING,
        column21 STRING,
        column22 STRING,
        column23 STRING,
        column24 STRING,
        column25 STRING, 
        column26 STRING,
        column27 STRING, 
        column28 STRING, 
        column29 STRING, 
        column30 STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{hdfs_path}'
    TBLPROPERTIES('skip.header.line.count'='1');

    CREATE TABLE {table_name} STORED AS PARQUET
      AS
    SELECT *
    FROM {tmp_table_name};
    
    DROP TABLE {tmp_table_name};

"""

HQL_CLEAN_DATA = """
    DROP TABLE IF EXISTS {clean_mydata};

    CREATE TABLE {clean_mydata} STORED AS PARQUET AS
    SELECT TRANSFORM (
       column1, column2, column3, column4, column5, column6,column7, column8, column9, column10,
       column11, column12, column13, column14, column15, column16, column17, column18, column19,
       column20, column21, column22, column23,column24, column25, column26,column27, column28,
       column29, column30, '{source_id}' 
    )
    USING 'python /opt/deploy/udfs/my_udf_function.py'

    AS (
        column1, column2, column3, column4, column5, column6,column7, column8, column9, column10, source_id
    )
    FROM {mydata}
"""

