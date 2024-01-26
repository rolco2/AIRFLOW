import logging
from datetime import datetime, timedelta
import airflow
from airflow import DAG
import pendulum
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {"owner": "Airflow", "start_date": datetime(2021,3,22,17,15)}

now = datetime.now()

#dag = DAG(
#    dag_id="snowflake_connector3", 
#    default_args=args, 
#    schedule_interval=None
#)

with DAG(
    dag_id = 'dags_python_with_snoflake',
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),      # 언제부터 시작할지 결정 / UTC 기준은  9시간 느리다(세계표준시간)
    schedule=None,                                                  # 분,시,일,월,요일 스케줄
    catchup=False   
) as dag:


    query1 = "INSERT INTO TEST_DB.PUBLIC.ERR_PROC_HIST  VALUES ('PARK', 'TB', 'EER'," + now + ");"


#def count1(**context):
#    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
#    result = dwh_hook.get_first("select count(*) from abcd_db.public.test3")
#    logging.info("Number of rows in `abcd_db.public.test3`  - %s", result[0])


    query1_exec = SnowflakeOperator(
        task_id="snowfalke_task1",
        sql= query1
        snowflake_conn_id="snowflake_conn"
    )

#    count_query = PythonOperator(task_id="count_query", python_callable=count1)
    query1_exec 