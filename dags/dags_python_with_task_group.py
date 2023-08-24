from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.utils.task_group import task_group

with DAG(
    dag_id="dags_python_with_task_group",                           # 화면에서 보이는 dag 이름
    schedule="10 9 * * *",                                          # 분,시,일,월,요일 스케줄
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),      # 언제부터 시작할지 결정 / UTC 기준은  9시간 느리다(세계표준시간)
#    start_date = datetime.date(2023, 3, 1),
#    schedule=None,
    catchup=False   
) as dag:

    def inner_func(**kwargs):
        msg = kwargs.get('msg') or ''
        print(msg)

    @task_group(group_id = 'first_group')  #그룹을 먼저 선언
    def group_1():
        ''' task_group 데커레이터를 이용한 첫 번쨰 그룹 '''
    
        @task(task_id = 'inner_function1')
        def inner_func1(**kwargs) :
            print( '첫 번째 TaskGroup 내 첫 번쨰 task')

        inner_function2 = PythonOperator(
            task_id = 'inner_function2',
            python_callable=inner_func,
            op_kwargs = {'mag' : '첫 번쨰 TaskGroup내 두번째 task 입니다.'}   
          )
        
        inner_func1() >> inner_function2
    
    
    with TaskGroup(group_id = 'second_group', tooltip = '두 번쨰 그룹') as group_2:
        ''' docstring은 표시되지 않습니다. '''
        @task(task_id = 'inner_function1')
        def inner_func1(**kwargs):
            print('두 번째 TaskGroup 내 첫 번째 task')
		
        inner_function2 = PythonOperator(
		    task_id = 'inner_function2',
			python_callable = inner_func,
			op_kwargs = {'msg' : '두 번쨰 TaskGroup 내 두 번쨰 task'}
		)
		
        inner_func1() >> inner_function2