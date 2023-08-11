from airflow import DAG
import pendulum
#import datetime
from airflow.operators.bash import BashOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_bash_python_with_xcom",                            # 화면에서 보이는 dag 이름
    schedule="10 0 * * *",                                          # 분,시,일,월,요일 스케줄
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),      # 언제부터 시작할지 결정 / UTC 기준은  9시간 느리다(세계표준시간)
    catchup=False   
) as dag:


	@task(task_id = 'python_push')
	def python_push_xcom():
		result_dict = {'status' : 'Good','data':[1,2,3], 'options_cnt' :100}
		return result_dict
		
	bash_pull = BashOperator (
		task_id = 'bash_pull',
		env={
			 'STATUS' : '{{ti.xcom_pull(task_ids="python_push") ["status"]}}',
			 'DATA'   : '{{ti.xcom_pull(task_ids = "python_push")["data"]}}',
			 'OPTIONS_CNT' : '{{ti.xcom_pull(task_ids = "python_push")["options_cnt"]}}'
			 },
		bash_command = 'echo $STATUS && echo $DATA && echo $OPTIONS_CNT'
	)

	python_push_xcom() >> bash_pull

	bash_push = BashOperator(
		task_id = 'bash_push',
		bash_command = 'echo PUSH_START '
		               '{{ti.xcom_push(key="bash_pushed",value=200)}} && '
			           'echo PUSH_COMPLETE'
	)     	

	@task(task_id ='python_pull')
	def python_pull_xcom(**kwargs):
		ti = kwargs['ti']
		status_value =  ti.xcom_pull(key = 'bash_pushed')
		return_value =  ti.xcom_pull(task_ids = 'bash_push')
 
		print('status_value:' + str(status_value))
		print('return_Value:' + return_value)

	bash_push >> python_pull_xcom()