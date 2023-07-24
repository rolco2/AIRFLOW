from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime
import pendulum

with DAG(
    dag_id="dags_bash_operator",                                # 화면에서 보이는 dag 이름
    schedule="0 0 * * *",                                       # 분,시,일,월,요일 스케줄
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),  # 언제부터 시작할지 결정 / UTC 기준은  9시간 느리다(세계표준시간)
    catchup=False,                                              # 만약 true로 설정되어있으면 2021.03.01 일에 실행하면 2021.01.01부터 누락된
                                                                # 일자 들도 전부 돌린다. 
#    dagrun_timeout=datetime.timedelta(minutes=60),             # 60분 이상 돌면 실패를 알린다. 
#    tags=["example", "example2"],                              # airflow UI에서 dag이름 밑에 써있는 문구
#    params={"example_key": "example_value"},                   # 공통적으로 보낼 파라메터 값을 설정
) as dag:
    
    bash_t1 = BashOperator(
        task_id="bash_t1",                        # airflow UI에서 graph에 나와잇는 명칭
        bash_command="echo whoami",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",                        # airflow UI에서 graph에 나와잇는 명칭
        bash_command="echo $HOSTNAME",
    )

    bash_t1 >> bash_t2