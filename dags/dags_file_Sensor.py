from airflow import DAG
import pendulum
from airflow.sensors.filesystem import FileSensor

with DAG(
        dag_id='dags_file_sensor',
        start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
        schedule='0 7 * * *',
        catchup=False
) as dag:
    
    tvCorona19VaccinestatNew_sensor = FileSensor(
        task_id = 'tvCorona19VaccinestatNew_sensor',
        fs_conn_id = 'conn_file_otp_airflow_files',
        filepath='tvCorona19vaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/tvCorona19VaccinestatNew.csv',
        recursive=False,
        poke_interval=60,
        timeout=60*60*24, # 1일
        mode='reschedule'
    )

tvCorona19VaccinestatNew_sensor