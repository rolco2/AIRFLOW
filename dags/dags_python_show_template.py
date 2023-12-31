from airflow import DAG
import pendulum
#import datetime
from airflow.decorators import task

with DAG(
    dag_id= 'dags_python_show_template',
    schedule = "30 9 * * *",
    start_date = pendulum.datetime(2023, 7, 1, tz = "Asia/Seoul"),
    catchup = True
) as dag:

    @task(task_id = 'python_task')
    def show_tempates(**kwargs):
        from pprint import pprint
        pprint(kwargs)

    show_tempates()