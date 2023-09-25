from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pendulum
from airflow.models import Variable

email_str = Variable.get("email_target")
email_lst = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_sla_email_example',
    start_date=pendulum.datetime(2023, 5, 1, tz='Asia/Seoul'),
    schedule='*/10 * * * *',
    catchup=False,
    default_args={
        'sla': timedelta(seconds=70),   # dag 시작 시점으로 70초 까지 
        'email': email_lst
    }
) as dag:
    
    task_slp_30s_sla_70s = BashOperator(
        task_id='task_slp_30s_sla_70s',   
        bash_command='sleep 30'         # 실행시간 30초 / sla 70초라 성공
    )
    
    task_slp_60_sla_70s = BashOperator(
        task_id='task_slp_60_sla_70s',
        bash_command='sleep 60'         # 실행 시간 60초 / 첫번째 30 + 60=  90초 실패
    )

    task_slp_10s_sla_70s = BashOperator(
        task_id='task_slp_10s_sla_70s',
        bash_command='sleep 10'        # 실행 시간 10초 / 두번쨰 부터 실패 처리
    )

    task_slp_10s_sla_30s = BashOperator(
        task_id='task_slp_10s_sla_30s',
        bash_command='sleep 10',      # 실행 시간 10초 
        sla=timedelta(seconds=30)     # 30초로 지정했지만 결국 30+60+10 초로 sla 끝나기전이라 실패처리 
    )                                 # 제일 먼저 메일이 발송 될 것이다.

    task_slp_30s_sla_70s >> task_slp_60_sla_70s >> task_slp_10s_sla_70s >> task_slp_10s_sla_30s