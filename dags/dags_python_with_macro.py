from airflow import DAG
import pendulum
#import datetime
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_macro",                                # 화면에서 보이는 dag 이름
    schedule="10 0 * * *",                                          # 분,시,일,월,요일 스케줄
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),      # 언제부터 시작할지 결정 / UTC 기준은  9시간 느리다(세계표준시간)
    catchup=False,         
) as dag:
    
    @task(task_id = 'task_using_macros',
      templates_dict ={'start_date':'{{ (data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months = -1, day = 1)) | ds }}',
	                   'end_date':'{{(data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds }}'
	                  }
	 )
	 
    def get_detetime_macro(**kwargs) :
        template_dict = kwargs.get('templates_dict') or {}  # task에 templates_dict 파라미터값이 template_dict에 전부 전달
        if template_dict:                                   # 빈값이 아니면 실행
            start_date = template_dict.get('start_date') or 'start_date없음'
            end_date   = template_dict.get('end_date') or 'end_date없음'
            
            print(start_date)
            print(end_date)


    @task(task_id = 'task_direct_calc')
    def get_Datetime_calc(**kwargs):
        from dateutil.relativedelta import relativedelta
        
        data_interval_end = kwargs['data_interval_end']
        
        pre_month_day_first = data_interval_end.in_timezone('Asia/Seoul') + relativedelta(months= -1, day =1)
        pre_month_day_last  = data_interval_end.in_timezone('Asia/Seoul').replace(days = 1) + relativedelta(days = -1)
        
        print(pre_month_day_first.strftime('%Y-%m-%d'))
        print(pre_month_day_last.strftime('%Y-%m-%d'))


    get_detetime_macro() >> get_Datetime_calc()