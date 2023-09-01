from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimplehttpOperator
from airflow.decorators import task
    
with DAG(
    dag_id = 'dags_simple_http_operator',
    schedule="10 9 * * *",                                          # 분,시,일,월,요일 스케줄
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),      # 언제부터 시작할지 결정 / UTC 기준은  9시간 느리다(세계표준시간)
#    start_date = datetime.date(2023, 3, 1),
#    schedule=None,
    catchup=False 
) as dag:
    
    ''' 서울시 공공자전거 대여소 정보 '''
    tb_cycle_station_info = SimplehttpOperator(
       task_id = 'tb_cycle_station_info',
       http_conn_id = 'openAPI.seoul.go.kr',   #airflow UI에서 connection 이름
      # endpoint = '534b734a62677977343351564a5647/json/tbCycleStationInfo/1/10/',
      # 인증키값을 직접 적으면 여러 문제가 발생할 수 있음으로 variable로 변경해준다. 
       endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/tbCycleStationInfo/1/10/', #airflow에 variable 사용하여 템플릿 이용해 작성
       method = 'GET'
       headers = {'Content-Type: 'application/json',
                  'charset' : 'utf-8',
                  'Accept'  : '*/*'
                 }                  
    )

    @task(task_id ='python_2')
    def python_2(**kwargs):
        import json 
        from pprint import pprint
        
        ti = kwargs['ti'] 
        rslt = ti.xcom_pull(task_ids='tb_Cycle_station_info')
        
        pprint(json.loads(rslt))
        
    tb_cycle_station_info >> python_2()