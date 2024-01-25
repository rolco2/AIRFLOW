from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.branch import BaseBranchOperator

with DAG(
    dag_id='dags_base_branch_operator',
    schedule="10 9 * * *",                                            # 분,시,일,월,요일 스케줄
    start_date = pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),      # 언제부터 시작할지 결정 / UTC 기준은  9시간 느리다(세계표준시간)
    catchup=False 
) as dag:
    
    class CustomBranchOperator(BaseBranchOperator) :
        def choose_branch(self, context):             # choose_branch 이름을 바꾸면안된다. context 파라메터도 바꾸면안된다)
            import random
            print(context)
            
            item_lst = ['A','B','C']
            selected_item = random.choice(item_lst)
            if selected_item == 'A' :
                return 'task_a'
            elif selected_item == 'B' :
                return ['task_b','task_c']
    
    custom_branch_operator = CustomBranchOperator(task_id = 'python_branch_task')
    
    def common_func(**kwargs):
        print(kwargs['selected'])
        
    task_a = PythonOperator(
        task_id = 'task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}    
    )
    
    task_b = PythonOperator(
        task_id = 'task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}    
    )
    
    task_c = PythonOperator(
        task_id = 'task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}    
    )
    
    custom_branch_operator >> [task_a, task_b, task_c]