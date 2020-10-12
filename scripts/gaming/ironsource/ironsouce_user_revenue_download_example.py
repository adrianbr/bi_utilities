import requests 
import json

def get_bearer_token(secretkey, refreshtoken):
    """https://developers.ironsrc.com/ironsource-mobile/air/authentication/#step-1"""
    url = 'https://platform.ironsrc.com/partners/publisher/auth'
    headers = {'secretkey': secretkey,
           'refreshToken': refreshtoken}

    response = requests.get(url, headers=headers)
    return response.text


def get_ironsrc_data_url(token_, appkey):
    """https://developers.ironsrc.com/ironsource-mobile/air/user-ad-revenue-v2/#step-1"""

    headers = {'Authorization': 'Bearer ' + token_.replace('"', '')}

    url_request = "https://platform.ironsrc.com/partners/userAdRevenue/v3?appKey={key}&date={date_iso_string}&reportType=1".format(key=appkey, date_iso_string = '2020-02-20')

   
    resp = requests.get(url_request, headers=headers)
    urls = json.loads(resp.text).get('urls')
    return urls


def download_ironsrc_data(secretkey, refreshtoken, appkey):
    """ returns file name so you can pass this to your loading function"""
    token = get_bearer_token(secretkey, refreshtoken)
    urls = get_ironsrc_data_url(token, appkey)
    url = urls[0]
    filename = 'filename.gz'
    filedata = requests.get(url)
    with open(filename, 'wb') as f:
        f.write(filedata.content)
    return filename

if __name__ == "__main__":
    secretkey = 'yoursecretkey'
    refreshtoken = 'refreshtoken'
    appkey = 'appkey'
    filename = download_ironsrc_data(secretkey, refreshtoken, appkey)
    #do loading logic
    print(filename)
