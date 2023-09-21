from airflow import Dataset
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

''' 여기서는 dags_dataset_producer_1에 있는 키값을 꺼내서 저장하고 있다.'''
dataset_dags_dataset_producer_1 = Dataset("dags_dagaset_producer_1")  #dags안에 있는 task 값이 큐에 전달하는 키 값

with DAG(
        dag_id="dags_dataset_consumer_1",                           # 화면에서 보이는 dag 이름
        schedule= [dataset_dags_dataset_producer_1],                # 키값을 구독하고있어 키값의 dag이 완료되면 실행                                  # 분,시,일,월,요일 스케줄
        start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),  # 언제부터 시작할지 결정 / UTC 기준은  9시간 느리다(세계표준시간)
        catchup=False,                                              # 만약 true로 설정되어있으면 2021.03.01 일에 실행하면 2021.01.01부터 누락된
) as dag:
    
    bash_task = BashOperator(
        task_id = 'bash_task',
        bash_command='echo {{ti.run_id}} && echo"producer_1 dl 이 완료되면 수행"'
    )