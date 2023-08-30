from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id = 'dags_trigger_dag_run_operator',
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),      # 언제부터 시작할지 결정 / UTC 기준은  9시간 느리다(세계표준시간)
    schedule="10 9 * * *",                                          # 분,시,일,월,요일 스케줄
    catchup=False   
) as dag:
	
	start_task = BashOperator(
	    task_id = 'start_task',
		bash_command = 'echo "start!"'	
	)
	
	trigger_dag_task = TriggerDagRunOperator(
	    task_id = 'trigger_dag_task',                 # 필수 파라메터
		trigger_dag_id = 'dags_python_operator',     # 실행시킬 dag_id (필수 파라메터)
		trigger_run_id = None,
		execution_date = '{{data_interval_end }}',
		reset_dag_run=True,
		wait_for_completion = False,
		poke_interval=60,
		allowed_states=['success'],
		failed_states=None	
	)
	
	start_task >> trigger_dag_task
	