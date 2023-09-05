from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd

class SeoulApitoCsvOperator(BaseOperator):
    template_fields = ('endpoint', 'path', 'file_name','base_dt' )

    def __init__(self, dataset_nm, path, file_name, base_dt = None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'openAPI.seoul.go.kr'
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm
        self.base_dt = base_dt
        
    def execute(self, context):
        import os
        from pprint import pprint

        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'{connection.host}:{connection.port}/{self.endpoint}'

        total_row_df = pd.DataFrame()
        start_row = 1
        end_row = 1000
        while True :
            self.log.info(f'시작 : {start_row}')
            self.log.info(f'끝 : {end_row}')
            print('aaaaaaaaaaaaaaa')
            row_df = self._call_api(self.base_url, start_row, end_row)
            print('bbbbbbbbbbbbbbbbbb')
            total_row_df = pd.concat([total_row_df, row_df])
            if len(row_df) < 1000 :
                break
            else :
                start_row = end_row + 1
                end_row += 1000
        
        if not os.path.exists(self.path):
            os.system(f'mkdir -p {self.path}')
        
        total_row_df.to_csv(self.path + '/' + self.file_name, encoding = 'utf-8', index=False)

    def _call_api(self, base_url, start_row, end_row):
        import requests
        import json
        
        headers = {'content-Type' : 'application/json',
                   'charset' : 'utf-8',
                   'Accept' : '*/*'                   
                   }
        request_url = f'{base_url}/{start_row}/{end_row}/'                     #API에서 가져오는 형식
        print(request_url)


    #    if self.base_dt is not None:
    #        request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}'
        
        print(request_url)

        response = requests.get(request_url, headers)     #http에 get 요청
        contents = json.loads(response.text)             #딕셔너리로 변환

        print(response)
        print(contents)

        key_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm).get('row')

        row_df = pd.DataFrame(row_data)

        return row_df