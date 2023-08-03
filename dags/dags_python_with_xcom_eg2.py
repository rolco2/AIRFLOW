from airflow import DAG
import pendulum
#import datetime
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_xcom_eg2",                                # 화면에서 보이는 dag 이름
    schedule="10 0 * * *",                                          # 분,시,일,월,요일 스케줄
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),      # 언제부터 시작할지 결정 / UTC 기준은  9시간 느리다(세계표준시간)
    catchup=False,         
) as dag:
    
    @task(task_id = 'python_xcom_push_by_return')
    def xcom_push_result(**kwargs):
        return 'Success'

    @task(task_id = 'python_xcom_pull_1')
    def xcom_pull_1(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(task_ids = 'python_xcom_push_by_return')
        print('xcom_pull 메서드로 직접 찾은 리턴 값 :' + value1)

    @task(task_id = 'python_xcom_pull_2')
    def xcom_pull_2(status, **kwargs):
        print('함수 입력값으로 받은 값 :' + status)


    python_xcom_push_return = xcom_push_result()
    xcom_pull_2(python_xcom_push_return)
    
    python_xcom_push_return >> xcom_pull_1()