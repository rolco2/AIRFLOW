from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.bash   import BashOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_trigger_rule_eg2",                     # 화면에서 보이는 dag 이름
    schedule="10 9 * * *",                                          # 분,시,일,월,요일 스케줄
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),      # 언제부터 시작할지 결정 / UTC 기준은  9시간 느리다(세계표준시간)
#    start_date = datetime.date(2023, 3, 1),
#    schedule=None,
    catchup=False   
) as dag:

    @task.branch(task_id = 'branching')
    def select_random():
        import random

        item_lst = ['A','B','C']
        selected_item = random.choice(item_lst)
        if selected_item == 'A' :
            return 'task_a'
        elif selected_item == 'B' :
            return 'task_b'
        elif selected_item == 'C' :
            return 'task_c'
                  
      
    task_a = BashOperator(
        task_id = 'task_a',
        bash_command = 'echo upstrem1'
    )
      
    @task(task_id = 'task_b')
    def task_b():
        print('정상 처리')
      
    @task(task_id = 'task_c')
    def task_c():
        print('정상 처리')
            
    @task(task_id = 'task_d', trigger_rule='none_nskipped')
    def task_d() :
        print('정상 처리')
            
    select_random() >> [task_a,task_b(),task_c()] >> task_d()
