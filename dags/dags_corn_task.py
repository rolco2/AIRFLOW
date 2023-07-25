from airflow import DAG
from airflow.operators.empty import EmptyOperator
#import detetime
import pendulum



with DAG(
    dag_id="dags_corn_task",                                    # 화면에서 보이는 dag 이름
    schedule= None,                                       # 분,시,일,월,요일 스케줄
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),  # 언제부터 시작할지 결정 / UTC 기준은  9시간 느리다(세계표준시간)
    catchup=False,                                              # 만약 true로 설정되어있으면 2021.03.01 일에 실행하면 2021.01.01부터 누락된
                                                                # 일자 들도 전부 돌린다. 
#    dagrun_timeout=datetime.timedelta(minutes=60),             # 60분 이상 돌면 실패를 알린다. 
#    tags=["example", "example2"],                              # airflow UI에서 dag이름 밑에 써있는 문구
#    params={"example_key": "example_value"},                   # 공통적으로 보낼 파라메터 값을 설정
) as dag:
    
    t1 = EmptyOperator(
        task_id = "t1"
    )
    t2 = EmptyOperator(
        task_id = "t2"
    )
    t3 = EmptyOperator(
        task_id = "t3"
    )
    t4 = EmptyOperator(
        task_id = "t4"
    )
    t5 = EmptyOperator(
        task_id = "t5"
    )
    t6 = EmptyOperator(
        task_id = "t6"
    )
    t7 = EmptyOperator(
        task_id = "t7"
    )
    t8 = EmptyOperator(
        task_id = "t8"
    )

    t1 >> [t2, t3] >> t4
    t5 >> t4
    [t4, t7] >> t6 >> t8