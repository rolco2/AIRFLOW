from airflow import DAG
import pendulum
#import datetime
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="dags_bash_with_macro_eg1",                                # 화면에서 보이는 dag 이름
    schedule="10 0 L * *",                                       # 분,시,일,월,요일 스케줄
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),  # 언제부터 시작할지 결정 / UTC 기준은  9시간 느리다(세계표준시간)
    catchup=False,         
) as dag:

    # START_DATE : 전월, 말일, END_DATE : 1일전
    bash_task_1 = BashOperator (
        task_id = 'bash_task_1',
        env ={'START_DATE' : '{{ data_interval_start.in_timzone("Asia/Seoul") | ds }}',
              'END_dATE': '{{ (data_inverval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days -1)) | ds }}'
              },
        bash_command= 'echo "START_dATE : $START_DATE" && echo "END_DATE : $END_DATE"'
    )