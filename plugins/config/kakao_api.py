import pendulum
from dateutil.relativedelta import relativedelta
import os
import json
import requests
from airflow.models import Variable

REDIRECT_URL = 'https://example.com/oauth'

def _refresh_token_to_variable():
    ''' airflow 에서 varialbles에 정의한 내용을 꺼내온다. '''
    client_id = Variable.get("kakao_client_secret")
    tokens = eval(Variable.get("kakao_tokens"))
    refresh_token = tokens.get('refresh_token')
    url = "https://kauth.kakao.com/oauth/token"
    data = {
        "grant_type": "refresh_token",
        "client_id": f"{client_id}",
        "refresh_token": f"{refresh_token}"
    }
    response = requests.post(url, data=data)
    rslt = response.json()
    new_access_token = rslt.get('access_token')
    new_refresh_token = rslt.get('refresh_token')         # Refresh 토큰 만료기간이 30일 미만이면 refresh_token 값이 포함되어 리턴됨.
    ''' 아래 if에서 새로운 토큰정보로 바뀐다. '''
    if new_access_token:
        tokens['access_token'] = new_access_token
    if new_refresh_token:
        tokens['refresh_token'] = new_refresh_token

    now = pendulum.now('Asia/Seoul').strftime('%Y-%m-%d %H:%M:%S')
    tokens['updated'] = now                               # 토큰 update가 잘되었는지는 여기서 확인
    os.system(f'airflow variables set kakao_tokens "{tokens}"')
    print('variable 업데이트 완료(key: kakao_tokens)')



def send_kakao_msg(talk_title: str, content: dict):
    '''
    특성상 총 3개의 title만 만들 수 있다.
    content:{'tltle1':'content1', 'title2':'content2'...}
    '''

    try_cnt = 0
    while True:
        ### get Access 토큰
        tokens = eval(Variable.get("kakao_tokens")) #airflow의 variables에 딕셔너리 형태로 저장되어 있어 딕셔너리로 저장.
        access_token = tokens.get('access_token')
        content_lst = []
        button_lst = []

        for title, msg in content.items(): #content 형태가 딕셔너리라 각각 key, value를 가져 온다 
            content_lst.append({
                'title': f'{title}',
                'description': f'{msg}',
                'image_url': '',
                'image_width': 40,
                'image_height': 40,
                'link': {
                    'web_url': '',
                    'mobile_web_url': ''
                }
            })

            #버튼은 없으면 오류 되기에 빈값으로 만든다.
            button_lst.append({
                'title': '',
                'link': {
                    'web_url': '',
                    'mobile_web_url': ''
                }
            })

        list_data = {
            'object_type': 'list',
            'header_title': f'{talk_title}',
            'header_link': {
                'web_url': '',
                'mobile_web_url': '',
                'android_execution_params': 'main',
                'ios_execution_params': 'main'
            },
            'contents': content_lst,
            'buttons': button_lst
        }

        send_url = "https://kapi.kakao.com/v2/api/talk/memo/default/send"
        headers = {
            "Authorization": f'Bearer {access_token}' #airflow variables 에서 정의한 acess_token
        }
        data = {'template_object': json.dumps(list_data)} #위 list 데이터를 json으로 변환
        response = requests.post(send_url, headers=headers, data=data)  # api를 호출
        print(f'try횟수: {try_cnt}, reponse 상태:{response.status_code}')
        try_cnt += 1

        if response.status_code == 200:         # 200: 정상
            return response.status_code
        elif response.status_code == 400:       # 400: Bad Request (잘못 요청시), 무조건 break 하도록 return
            return response.status_code
        elif response.status_code == 401 and try_cnt <= 2:      # 401: Unauthorized (토큰 만료 등)
            _refresh_token_to_variable()
        elif response.status_code != 200 and try_cnt >= 3:      # 400, 401 에러가 아닐 경우 3회 시도때 종료
            return response.status_code