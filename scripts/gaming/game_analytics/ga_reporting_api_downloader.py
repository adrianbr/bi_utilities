import requests
import json

""" 
-- metrics
-- games
-- studio
"""

def _get_api_key():
    """returns the header variable we need to send for authentication
       here we retrieve it from env"""
    import os
    #run on your mac: export ga_key="yourkey"
    key = os.environ.get('ga_key')
    header_auth_dict = {'X-API-Key': key}
    return header_auth_dict

def make_request(endpoint, params={}):
    url = f"https://metrics.gameanalytics.com/metrics/v1/{endpoint}"
    credentials = _get_api_key()

    response = requests.get(url, headers=credentials, params=params)
    return response.text

def get_available_metrics():
    data = make_request('metrics')
    return json.loads(data)

def get_available_games():
    interval = "2020-10-15T00:00:00.000/2020-10-16T00:00:00.000"
    params = {'interval': interval}
    data = make_request('games', params=params)
    return json.loads(data)


a = get_available_games()
print(a)
for i in a:
    print (i)