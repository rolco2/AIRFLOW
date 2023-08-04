from airflow import DAG
import pendulum
#import datetime
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="dags_bash_with_xcom",                                    # 화면에서 보이는 dag 이름
    schedule="10 0 * * *",                                          # 분,시,일,월,요일 스케줄
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),      # 언제부터 시작할지 결정 / UTC 기준은  9시간 느리다(세계표준시간)
    catchup=False,         
) as dag:
    
    bash_push = BashOperator (
        task_id = 'bash_push',
        bash_command =  "echo START &&"
                        "echo XCOM_PUSHED "
                        "{{ ti.xcom_push(key = 'bash_pushed', value = 'first_bash_message') }} && "
                        " echo COMPLATE" 
    )

    bash_pull = BashOperator(
        task_id = 'bash_pull' ,
        env ={'PUSHED_VALUE' : "{{ ti.xcom_pull (key='bash_pushed') }}",
              'RETURN_VALUE' : "{{ ti.xcom_pull(task_ids = 'bash_push') }}" },
        bash_command = "echo $PUSHED_VALUE && echo $RETURN_VALUE ",
        do_xcom_push =False    ## bash_command의 값을 RETURN_VALUE에 저장안한다.
    )

    bash_push >> bash_pull
