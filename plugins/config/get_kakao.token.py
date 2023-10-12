import requests

'''client_id와 인가 코드는 값을 해킹당하면 본인 카카오 계정이 해킹당할 수 있다.'''

client_id = '0431d98065dd032d5cfa1d3519da6d69'
redirect_uri = 'https://www.example.com/oauth'
authorize_code = 'KzeMWDIp6ywYrXowfH6rQ7r3A1ZubmWaYHfHLtQL12_N7pNzQgnBxb6MpBdhwx-i-08FVworDNQAAAGLHG-daA'


token_url = 'https://kauth.kakao.com/oauth/token'
data = {
    'grant_type': 'authorization_code',
    'client_id': client_id,
    'redirect_uri': redirect_uri,
    'code': authorize_code,
    }

response = requests.post(token_url, data=data)
tokens = response.json()
print(tokens)