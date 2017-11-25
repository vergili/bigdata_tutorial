import os
import logging
from airflow.hooks.hive_hooks import BaseHook, HiveServer2Hook, HiveCliHook
from subprocess import Popen
import hashlib


logger = logging.getLogger(__name__)


def create_source_id(templates_dict, **kwargs):

    source = templates_dict['source']
    ds = kwargs['ds']
    source_id = hashlib.md5('-'.join([source, str(ds)])).hexdigest()

    return source_id


def count_data_rows(templates_dict, **kwargs):

    hook = HiveServer2Hook()

    query = """
        SELECT count(*) FROM mydata
    """
    result = hook.get_results(schema=templates_dict['schema'], hql=query)

    if result['data'][0][0] > 100:

        return 'clean_data'
    else:
        return 'stop_flow'


def move_data_mssql(templates_dict, **kwargs):

    tmp_table = 'clean_mydata_tmp'
    table = 'clean_mydata'
    delimiter = "','"
    quote_char = "'\"'"
    escape_char = "'\\\\'"
    number_of_mappers = '4'
    mssql_table = 'mydata'

    # -------Convert Hive Table from PARQUET to TEXT --------------------------------------------

    hook = HiveCliHook()

    query = """
        DROP TABLE {tmp_table};

        CREATE TABLE {tmp_table}
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            WITH SERDEPROPERTIES (
               "separatorChar" = {delimiter},
               "quoteChar"     = {quote_char},
               "escapeChar"    = {escape_char}
            )
        STORED AS TEXTFILE
        AS
        SELECT column1, column2, column3, column4, column5, column6,column7, column8, column9, column10, source_id
        FROM {table}

    """.format(tmp_table=tmp_table, table=table,
               delimiter=delimiter, quote_char=quote_char, escape_char=escape_char)

    hook.run_cli(schema=templates_dict['schema'], hql=query)

    # --------------Run sqoop--------------------------------------------------------------------

    # get default mssql connection
    sql_conn = BaseHook.get_connection('mssql_default')
    conn_str = "'jdbc:sqlserver://{host};databaseName={database}'".format(
        host=sql_conn.host,
        database=sql_conn.extra_dejson.get('database')
    )

    # get default hive cli connection
    hive_conn = BaseHook.get_connection('hive_cli_default')
    hdfs_export_dir = 'hdfs://{host}:{port}/user/hive/warehouse/my_hive_db.db/{table}'.format(
        host=hive_conn.host,
        port=8020,
        table=tmp_table
    )

    cmd = ['/usr/bin/sqoop', 'export', '--connect', conn_str,
           '--username', sql_conn.login, '--password', sql_conn.password,
           '--table', mssql_table,
           '--export-dir', hdfs_export_dir,
           '-m', number_of_mappers,
           '--input-fields-terminated-by', delimiter,
           '--input-enclosed-by', quote_char,
           '--input-escaped-by', escape_char,
           '--input-null-string', "'\\\N'"]
    cmd = ' '.join(cmd)

    print cmd

    logging.info("Executing sqoop")
    sp = Popen(cmd, shell=True)

    sp.wait()
    logging.info("Command exited with return code {0}".format(sp.returncode))


