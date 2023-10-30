url = 'https://api.openai.com/v1/chat/completions'
secret_key = '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'
headers = {'Authorization': f'Bearer {secret_key}' }  # json으로 받기 떄문에 'Content-Type': 'application/json' 생략


data = {
        "model": "gpt-3.5-turbo",
        "messages": [{"role": "user", "content": "Say this is a test!"}],
        "temperature": 0.7
      }
print(headers)


import requests 
import json

resp = requests.post(url=url, headers=headers, json = data)
resp_dict = json.loads(resp.text)
# 아래부분은 현재 billing 문제로 나오지가 않음
#resp_text = resp_dict['choices'][0]['message']['content']  # json 형식의 데이터를 맞게 꺼내와야한다.
print(resp_dict)