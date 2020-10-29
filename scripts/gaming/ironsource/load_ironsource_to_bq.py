from bq_upload import local_ironscr_file_to_bq
import ironsouce_user_revenue_download
import os

#ironsource creds
secretkey = os.environ.get('sc1')
refreshtoken = os.environ.get('sc2')


filenames = ironsouce_user_revenue_download.download_ironscr_data_all_games(secretkey, refreshtoken)
for filename in filenames:

    print(filename)
    config = { }
    config['local_file'] = filename
    config['bucket'] = 'datateam_bucket'
    config['filename_on_bucket'] = config['local_file']
    config['dataset'] = 'get-data-team.tenjin_dv_test'
    config['table_name'] = config['local_file'].replace('.gz','')

    local_ironscr_file_to_bq(**config )
    print(f"Successfuly loaded to {config['dataset'] + config['table_name']}!")
