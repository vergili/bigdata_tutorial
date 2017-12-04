import logging
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.sensors import WebHdfsSensor
from adflow.ingestion.tutorial import tasks
from adflow.ingestion.tutorial import hql


logger = logging.getLogger(__name__)

DAG_ID = 'my-bigdata-dag'

default_args = {
    'owner': 'Mehmet Vergili',
    'start_date': datetime(2017, 11, 20),
    'depends_on_past': False,
    'email': 'mehmet.vergili@gmail.com',
    'email_on_failure': 'mehmet.vergili@gmail.com',
    'email_on_retry': 'mehmet.vergili@gmail.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}

dag = DAG(dag_id=DAG_ID,
          default_args=default_args,
          schedule_interval=timedelta(days=1))

source_data_sensor = WebHdfsSensor(
    task_id='source_data_sensor',
    filepath='/data/mydata/{{ ds }}/mydata.csv',
    poke_interval=10,
    timeout=5,
    dag=dag
)

create_hive_db = HiveOperator(
    task_id='create_hive_db',
    hql="DROP DATABASE IF EXISTS {db} CASCADE; CREATE DATABASE {db};".format(db='my_hive_db'),
    provide_context=True,
    dag=dag
)
create_hive_db.set_upstream(source_data_sensor)

hdfs_to_hive_trasfer = HiveOperator(
    task_id='hdfs_to_hive_trasfer',
    hql=hql.HQL_HDFS_TO_HIVE_TRANSFER.format(table_name='mydata',
                                             tmp_table_name='mydata_tmp',
                                             hdfs_path='/data/mydata/{{ ds }}'),
    schema='my_hive_db',
    provide_context=True,
    dag=dag
)
hdfs_to_hive_trasfer.set_upstream(create_hive_db)


count_data_rows = BranchPythonOperator(
    task_id='count_data_rows',
    python_callable=tasks.count_data_rows,
    templates_dict={'schema': 'my_hive_db'},
    provide_context=True,
    dag=dag
)
count_data_rows.set_upstream(hdfs_to_hive_trasfer)


stop_flow = DummyOperator(
    task_id='stop_flow',
    dag=dag
)

create_source_id = PythonOperator(
    task_id='create_source_id',
    python_callable=tasks.create_source_id,
    templates_dict={'source': 'mydata'},
    provide_context=True,
    dag=dag
)
create_source_id.set_upstream(source_data_sensor)


clean_data = HiveOperator(
    task_id='clean_data',
    hql=hql.HQL_CLEAN_DATA.format(source_id="{{ task_instance.xcom_pull(task_ids='create_source_id') }}",
                                  clean_mydata='clean_mydata', mydata='mydata'),
    schema='my_hive_db',
    provide_context=True,
    dag=dag
)
clean_data.set_upstream(create_source_id)
count_data_rows.set_downstream([stop_flow, clean_data])


move_data_mysql = PythonOperator(
    task_id='move_data_mysql',
    python_callable=tasks.move_data_mssql,
    templates_dict={'schema': 'my_hive_db'},
    provide_context=True,
    dag=dag
)
move_data_mysql.set_upstream(clean_data)


send_email = EmailOperator(
    task_id='send_email',
    to='mehmet.vergili@gmail.com',
    subject='ingestion complete',
    html_content="Date: {{ ds }}",
    dag=dag)

send_email.set_upstream(move_data_mysql)




