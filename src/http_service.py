import json
import requests


def get_response(request):
    print(request)
    if request['method'] == 'POST':
        response = requests.post(request['url'], data=request['data'], params=request['params']
                                 , headers=request['headers'])
    else:
        response = requests.get(request['url'], params=request['params'], headers=request['headers'])
    return {'payload': response.json(), 'status': response.status_code}
