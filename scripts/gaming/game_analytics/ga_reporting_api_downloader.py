import requests
import json
import datetime

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
    #print (response.status_code)
    #print (response.text)
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

def get_interval(days_lookback=3):
    data = make_request('interval')
    data = data.replace('"','')
    last_date = datetime.datetime.strptime(data.split("/")[1][:10], '%Y-%m-%d')
    #print(last_date)
    start_date = last_date - datetime.timedelta(days=days_lookback)
    new_interval = start_date.strftime('%Y-%m-%d') + data[10:]
    #print(new_interval)
    return new_interval

def get_available_games(interval=None):
    if interval is not None:
        interval = get_interval()#"2020-10-15T00:00:00.000/2020-10-16T00:00:00.000"
    params = {'interval': interval}
    data = make_request('games', params=params)
    return json.loads(data)




def get_retention_retro_data(interval, since_days_ago):
    endpoint = 'metrics/retention_retro'
    params = {}
    params['split_by'] = 'country_code'#split_by#"country_code, platform"
    params['interval'] = interval
    params['since_days_ago'] = since_days_ago
    games = get_available_games(interval)
    params['games'] = [game['id'] for game in games]

    data = json.loads(make_request(endpoint, params=params, request_type='post'))
    result = data.get('result')
    out_row_list = []
    for row in result:
        out_row = row.get('result')# result or event for other endpoints
        out_row['timestamp'] = row.get('timestamp')
        out_row['since_days_ago'] = since_days_ago
        out_row_list.append(out_row)

    return out_row_list



def get_new_users_data(interval):
    endpoint = 'metrics/new_users'
    query = {"dimensions": ['country_code', 'game_id'],
             "type": "split"}
    params = {}
    params['query'] = query
    params['interval'] = interval
    params['granularity'] = 'day'
    games = get_available_games(interval)
    params['games'] = [game['id'] for game in games]

    data = json.loads(make_request(endpoint, params=params, request_type='post'))
    result = data.get('result')
    rows = []
    for row in result:
        new_row = row.get('event')
        new_row['timestamp'] = row.get('timestamp')
        rows.append(new_row)

    return rows

def get_session_data(interval):
    endpoint = 'metrics/session_unique'
    query = {"dimensions": ['country_code', 'game_id'],
             "type": "split"}
    params = {}
    params['query'] = query
    params['interval'] = interval
    params['granularity'] = 'day'
    games = get_available_games(interval)
    params['games'] = [game['id'] for game in games]

    data = json.loads(make_request(endpoint, params=params, request_type='post'))
    result = data.get('result')
    #flatten row
    rows = []
    for row in result:
        new_row = row.get('event')
        new_row['timestamp'] = row.get('timestamp')
        rows.append(new_row)

    return rows

def _rows_to_json_file(rows, filename):
    with open(filename, 'w') as f:
        for row in rows:
            json.dump(row, f, default=str)
            f.write('\n')
    return filename


def download_data_and_make_files(days_lookback=3):
    interval = get_interval(days_lookback=days_lookback)

    #retention data
    retention_days = [1,7]
    data = []
    for day in retention_days:
        data += get_retention_retro_data(interval, day)

    retention_filename = _rows_to_json_file(data, 'retention_retro_data.json')

    #new users
    data = get_new_users_data(interval)
    new_users_filename = _rows_to_json_file(data, 'new_users_data.json')

    #sessions
    data = get_session_data(interval)
    session_filename = _rows_to_json_file(data, 'session_data.json')


    return [retention_filename, new_users_filename, session_filename]




files = download_data_and_make_files()
print(files)