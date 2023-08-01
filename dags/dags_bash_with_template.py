from airflow import DAG
import pendulum
#import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id = "dags_bash_with_template",
    schedule = "10 0 * * 6#1",
    start_date = pendulum.datetime(2023, 3, 1, tz = "Asia/Seoul"),
    catchup = False
) as dag:
    
    bash_t1 = BashOperator(
        task_id = 'bash_t1',
        bash_command= 'echo "data_interval_end: {{data_interval_end}} "' # 사용가능한 파라미터를 써야한다.
    )

    bash_t2 = BashOperator(
        task_id = 'bash_t2',
        env = {
            'START_DATE' : '{{data_interval_start | ds}}',
            'END_DATE' : '{{data_interval_end | ds}}'
        },
        bash_command = 'echo $START_DATE && echo $END_DATE'  # && < $STRT_DATE 성공하면 다음꺼 실행 (AND)
    )

    bash_t1 >> bash_t2