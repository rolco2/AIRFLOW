from operators.seoul_api_to_csv_operator import SeoulApitoCsvOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id = 'dags_seoul_api_corona',
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),      # 언제부터 시작할지 결정 / UTC 기준은  9시간 느리다(세계표준시간)
    schedule="10 9 * * *",                                          # 분,시,일,월,요일 스케줄
    catchup=False   
) as dag:
    
    ''' 서울시 코로나19 확진자 발생통합 '''
    tb_corona19_count_status = SeoulApitoCsvOperator(
        task_id = 'tb_corona19_count_status',
        dataset_nm = 'TbCorona19CountStatus',
        path = '/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        file_name = 'TbCorona19Countstatus.csv'
    )

    ''' 서울시 코로나19 백신 예방접종 현황 '''
    tv_corona19_vaccine_stat_new = SeoulApitoCsvOperator(
        task_id ='tv_corona19_vaccine_stat_new',
        dataset_nm = 'tvCorona19VaccinestatNew',
        path = '/opt/airflow/files/tvCorona19vaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        file_name = 'tvCorona19VaccinestatNew.csv'
    )

    tv_corona19_vaccine_stat_new
   # tb_corona19_count_status >> tv_corona19_vaccine_stat_new