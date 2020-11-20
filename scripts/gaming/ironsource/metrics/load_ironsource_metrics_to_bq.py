from bq_upload import local_ironscr_metrics_file_to_bq
import ironsource_metrics_download
import os
from datetime import date, timedelta

#ironsource creds
secretkey = os.environ.get('sc1')
refreshtoken = os.environ.get('sc2')


end_date = date.today().strftime('%Y-%m-%d')
start_date = (date.today() - timedelta(days=3)).strftime('%Y-%m-%d')
print(start_date)


filename = ironsource_metrics_download.get_ironsrc_metrics_file(secretkey, refreshtoken, start_date,end_date)
bucket = 'datateam_bucket'
filename_on_bucket = filename
dataset = 'get-data-team.tenjin_dv_test'
table_name = 'ironsource_metrics'


local_ironscr_metrics_file_to_bq(filename, bucket, filename_on_bucket, dataset, table_name)

print(f"Successfuly loaded to {dataset}.{table_name}!")
