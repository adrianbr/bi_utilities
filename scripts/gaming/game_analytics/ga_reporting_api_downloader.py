import requests
import json

""" 
https://apidocs.gameanalytics.com/metrics

"2012-01-01T00:00:00.000/2012-01-03T00:00:00.000"
-- metrics
-- games
-- studio
"""

def _get_api_key():
    """get key from your credential store"""
    import os
    #run on your mac: export ga_key="yourkey"
    key = os.environ.get('ga_key')
    header_auth_dict = {'X-API-Key': key}
    return header_auth_dict

def make_request(endpoint, params={}, request_type='get'):
    url = f"https://metrics.gameanalytics.com/metrics/v1/{endpoint}"
    credentials = _get_api_key()
    if request_type == 'get':
        response = requests.get(url, headers=credentials, params=params)
    elif request_type == 'post':
        response = requests.post(url, headers=credentials, json=params)
    print(url)
    print(params)
    print (response.status_code)
    print (response.text)
    print (response.json())
    return response.text

def get_available_metrics():
    data = make_request('metrics')
    return json.loads(data)

def get_available_dimensions():
    data = make_request('dimensions')
    return json.loads(data)

def get_available_dimension_values(dimension, interval, params={}):
    params[interval] = interval
    data = make_request(f'dimensions/{dimension}', params=params)
    return json.loads(data)['values']

def get_interval():
    data = make_request('interval')
    print(data)
    return data.replace('"','')

def get_available_games(interval=None):
    if interval is not None:
        interval = get_interval()#"2020-10-15T00:00:00.000/2020-10-16T00:00:00.000"
    params = {'interval': interval}
    data = make_request('games', params=params)
    return json.loads(data)

def get_returning_users_data(interval, since_days_ago):
    split_by = get_available_dimensions()
    endpoint = 'metrics/returning_users'
    params = {}
    params['split_by'] = "country_code" #split_by
    params['interval'] = interval
    params['since_days_ago'] = since_days_ago
    games = get_available_games(interval)
    params['games'] = [game['id'] for game in games]

    data = make_request(endpoint, params=params, request_type='post')
    return data


interval = "2020-11-11T16:00:00.000Z/2020-11-12T16:00:00.000Z"

a = get_returning_users_data(interval, 7)
print(a)
