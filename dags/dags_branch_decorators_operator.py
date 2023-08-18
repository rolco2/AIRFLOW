from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_branch_decorators_operator",                # 화면에서 보이는 dag 이름
    schedule="10 9 * * *",                                          # 분,시,일,월,요일 스케줄
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),      # 언제부터 시작할지 결정 / UTC 기준은  9시간 느리다(세계표준시간)
#    start_date = datetime.date(2023, 3, 1),
#    schedule=None,
    catchup=False   
) as dag:

    @task.branch(task_id = 'python_branch_task')
    def select_random():
        import random
        item_lst = ['A','B','C']
        selected_item = random.choice(item_lst)
        if selected_item == 'A':
            return 'task_a'
        elif selected_item in ['B','C']:
            return ['task_b','task_c']


    def common_func(**kwargs):
        print(kwargs['selected'])
        
    task_a = PythonOperator(
        task_id = 'task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}    
    )
    
    task_b = PythonOperator(
        task_id = 'task_b',
        python_Callable=common_func,
        op_kwargs={'selected':'B'}    
    )
    
    task_c = PythonOperator(
        task_id = 'task_c',
        python_Callable=common_func,
        op_kwargs={'selected':'C'}    
    )

    select_random() >> [task_a, task_b, task_c]