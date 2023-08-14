from airflow import DAG
import pendulum
#import datetime
from airflow.operators.bash import BashOperator
from airflow.models import Variable

with DAG(
    dag_id="dags_bash_with_variable",                            # 화면에서 보이는 dag 이름
    schedule="10 9 * * *",                                          # 분,시,일,월,요일 스케줄
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),      # 언제부터 시작할지 결정 / UTC 기준은  9시간 느리다(세계표준시간)
    catchup=False   
) as dag:
	
	var_value = Variable.get("sample_key")
	
	bash_var_1 = BashOperator(
		task_id = "bash_var_1",
		bash_command=f"echo variable:{var_value}"
	)
	
	bash_var_2 = BashOperator(
		task_id = "bash_var_2",
		bash_command = "echo variable:{{var.value.sample_key}}"
	)
	
  #  bash_var_1 >> bash_var_2