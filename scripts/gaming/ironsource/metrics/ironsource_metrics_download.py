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

    #impressions, clicks, completions, installs, spend, revenue
    #day, campaign, title, application, country, os,  deviceType, creative, adUnit
    #breakdowns = 'day,campaign,title,application,country,os,deviceType,creative,adUnit'

    #metrics = 'impressions,clicks,completions,installs,spend,revenue,eCPM'

    #url_request = f"""https://api.ironsrc.com/advertisers/v2/reports?breakdowns={breakdowns}&metrics={metrics}&format=json&startDate={start_date}&endDate={end_date}"""

    # v6 but getting status code 500
    breakdowns = 'date,app,country,platform,adUnits,placement'
    metrics = 'revenue,impressions,eCPM,activeUsers,engagedUsers,engagedUsersRate,impressionsPerEngagedUser,revenuePerActiveUser,revenuePerEngagedUser,clicks,clickThroughRate,completionRate,adSourceChecks,adSourceResponses,adSourceAvailabilityRate,sessions,engagedSessions,impressionsPerSession,impressionPerEngagedSessions,sessionsPerActiveUser'
    url_request = f"""https://platform.ironsrc.com/partners/publisher/mediation/applications/v6/stats?startDate={start_date}&endDate={end_date}&metrics={metrics}&breakdown={breakdowns}"""
    #url_request = "https://platform.ironsrc.com/partners/publisher/mediation/applications/v6/stats?startDate=2018-08-01&endDate=2018-08-01&metrics=revenue,impressions,eCPM&breakdown=adUnits"

    headers = {'Authorization': 'Bearer ' + token_.replace('"', '')}

    resp = requests.get(url_request, headers=headers)
    print(resp.status_code)

    data = resp.json()
    out = []

    for row in data:
        #get row dimensions
        dim_row = {key:value for key, value in row.items() if key !='data'}
        for data_row in row.get('data'):
            newrow = {**dim_row, **data_row}
            out.append(newrow)
        #print(row)
        #print(dim_row)
        #print(data_row)
        #print(newrow)

        #input("Press Enter to continue...")


    return out

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

    metrics = get_ironsrc_metrics_file(secretkey, refreshtoken, '2020-10-30', '2020-10-30')
    #print(metrics.encode('utf-8'))
    print('rows:',len(metrics))

    #for row in metrics:
        #print(json.dumps(row))
    #do loading logic
    #print(filename)
