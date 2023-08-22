from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.exceptions import AirflowException

with DAG(
    dag_id="dags_python_with_trigger_rule_eg1",                    # 화면에서 보이는 dag 이름
    schedule="10 9 * * *",                                          # 분,시,일,월,요일 스케줄
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),      # 언제부터 시작할지 결정 / UTC 기준은  9시간 느리다(세계표준시간)
#    start_date = datetime.date(2023, 3, 1),
#    schedule=None,
    catchup=False   
) as dag:

    bash_upstream_1 = BashOperator(
        task_id = 'bash_upstream_1',
        bash_command = 'echo upstream1'
    )
    
    @task(task_id = 'python_upstream_1')
    def python_upstream_1():
        raise AirflowException('downstream_1 Exception!')
    
    @task(task_id = 'python_upstream_2')
    def python_upstream_2():
        print('정상 처리')
    
    @task(task_id = 'python_downstream_1', trigger_rule = 'all_done')
    def python_downstream_1():
        print('정상 처리')
        
    [bash_upstream_1,python_upstream_1(),python_upstream_2()] >>  python_downstream_1()