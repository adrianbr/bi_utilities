import requests
import json
from datetime import datetime, timedelta

def get_bearer_token(secretkey, refreshtoken):
    """https://developers.ironsrc.com/ironsource-mobile/air/authentication/#step-1"""
    url = 'https://platform.ironsrc.com/partners/publisher/auth'
    headers = {'secretkey': secretkey,
           'refreshToken': refreshtoken}

    response = requests.get(url, headers=headers)
    #print(response.text)
    return response.text

def _rows_to_json_file(rows, filename):
    with open(filename, 'w') as f:
        for row in rows:
            json.dump(row, f, default=str)
            f.write('\n')
    return filename


def get_ironsrc_metrics(token_, start_date, end_date):
    """
    docs:  https://developers.ironsrc.com/ironsource-mobile/general/reporting-api-v2/#step-2
    token: bearer token
    start_date: iso date string eg "2010-10-30"
    end_date: same as start date
    :return data in list of json"""

    #impressions, clicks, completions, installs, spend
    #day, campaign, title, application, country, os,  deviceType, creative, adUnit

    url_request = f"""https://api.ironsrc.com/advertisers/v2/reports?\
breakdowns=day,campaign,title,application,country,os,deviceType,creative,adUnit\
&metrics=impressions,clicks,completions,installs,spend\
&format=json\
&startDate={start_date}\
&endDate={end_date}"""
    headers = {'Authorization': 'Bearer ' + token_.replace('"', '')}

    all_data = []
    print(url_request)
    #request the url above, and if there is a next page url, keep requesting
    while url_request:

        resp = requests.get(url_request, headers=headers)
        json_response = json.loads(resp.text.encode('utf8'))
        #print(json_response)
        #save data slice to out bucket
        data = json_response.get('data')
        if data:
            all_data += data

        #get the next link if available
        next_page_link = json_response.get('paging', {}).get('next')
        url_request = next_page_link
        print(url_request)

    #print(resp, resp.text)
    #print(urls)
    return all_data

def get_ironsrc_metrics_file(secretkey, refreshtoken, start_date, end_date):
    #get auth token
    token = get_bearer_token(secretkey, refreshtoken)

    data = get_ironsrc_metrics(token, start_date, end_date)

    filename =_rows_to_json_file(data, 'metrics.json')
    return filename



if __name__ == "__main__":
    import os
    secretkey = os.environ.get('sc1')
    refreshtoken = os.environ.get('sc2')

    metrics = get_ironsrc_metrics(token, '2020-10-30', '2020-10-30')
    #print(metrics.encode('utf-8'))
    print('rows:',len(metrics))

    fn = _rows_to_json_file(metrics, 'test.json')
    print(fn)
    #for row in metrics:
        #print(json.dumps(row))
    #do loading logic
    #print(filename)
