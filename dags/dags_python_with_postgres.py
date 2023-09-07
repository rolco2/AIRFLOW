from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum


with DAG(
    dag_id = 'dags_python_with_postgres',
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),      # 언제부터 시작할지 결정 / UTC 기준은  9시간 느리다(세계표준시간)
    schedule=None,                                                  # 분,시,일,월,요일 스케줄
    catchup=False   
) as dag:

    def insrt_postgres(ip, port, dbname, user, passwd, **kwargs):
        print("aaaaaaaaa")
        import psycopg2
        from contextlib import closing
        with closing(psycopg2.connect(host=ip, dbname=dbname, user=user, password=passwd, port=int(port))) as conn: 
            with closing(conn.cursor()) as cursor:  # with closing 은 cursor.close() 나 conn.close()를 생략할 수 있다
                dag_id = kwargs.get('ti').dag_id    # 'ti'는 task instance 로 이 정보들을 가져올수 있다.
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'insert 수행'
                sql = 'insert into py_opr_drct_insert values (%s,%s,%s,%s);'
                print(sql)
                cursor.execute(sql,(dag_id, task_id, run_id, msg))
                conn.commit()
                
    insert_postgres = PythonOperator(
        task_id = 'insert_postgres',
        python_callable = insrt_postgres,        
        op_args= ['127.28.0.3','5432','parkhj','parkhj','parkhj']
    )

    insrt_postgres