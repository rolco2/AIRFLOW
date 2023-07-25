from airflow import DAG
import pendulum
#import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id = "dags_bash_select_fruit",
    schedule = "10 0 * * 6#1",
    start_date = pendulum.datetime(2023, 3, 1, tz = "Asia/Seoul"),
    catchup = False
) as dag:
    
    ti_orange = BashOperator (
        task_id = "ti_orange",
        bash_command = "/opt/airflow/plugins/shell/select_fruit.sh ORANGE",
    )

    ti_avocado = BashOperator (
        task_id = "ti_avocado",
        bash_command = "/opt/airflow/plugins/shell/select_fruit.sh AVOCADO",
    )

    ti_orange >> ti_avocado