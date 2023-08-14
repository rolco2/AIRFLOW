from airflow import DAG
import pendulum
#import datetime
from airflow.operators.email import EmailOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_python_email_operator",                            # 화면에서 보이는 dag 이름
    schedule="0 8 1 * *",                                           # 분,시,일,월,요일 스케줄
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),      # 언제부터 시작할지 결정 / UTC 기준은  9시간 느리다(세계표준시간)
    catchup=False   
) as dag:

    @task(task_id = 'something_task')
    def some_logic(**kwargs):
        from random import choice
        return choice(['Success','Fail'])
        
        
    send_email = EmailOperator(
        task_id ='send_email',
        to = 'gywns9559@naver.com',
        subject = '{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} some_logic_처리결과',
        html_content = '{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} 처리 결과는 <br> \
                        {{ ti.xcom_pull(task_ids="something_task")}} 했습니다 <br>'

    )