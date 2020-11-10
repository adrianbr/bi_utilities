#the table configs are stored in a list of dicts.
#TODO: 'mopub_ad_impressions' logic
"""
example

table_configs = [
    {'increment_method' : 'increment_column' # or 'query'
     'increment_column': 'id', # which column to limit on
     'rows_per_increment' : 1000000, #optional, defaults to 1m

     'schema_name':'tenjin', # redshift schema to copy from
     'table_name': 'daily_ad_revenue', # redshift table
     'bucket': 'datateam_bucket', #your google storage bucket
     'dataset': 'get-data-team.tenjin_dv_test',
     'merge_columns_list':  ['id']
     }
     ,
     {'increment_method' : 'query',
     'query':"select count(*) from daily_ad_revenue where date >= CURRENT_DATE - interval '30 day'",

     'schema_name':'tenjin', # redshift schema to copy from
     'table_name': 'daily_ad_revenue', # redshift table
     'bucket': 'datateam_bucket', #your google storage bucket
     'dataset': 'get-data-team.tenjin_dv_test',
     'merge_columns_list':  ['date']
     }
]




table_configs = [

    {
    'increment_method': 'query',
    'query': "select * from daily_ad_revenue where date >= CURRENT_DATE - interval '3 day'",
    'schema_name': 'tenjin',
    'table_name': 'daily_ad_revenue',
    'bucket': 'datateam_bucket',
    'dataset': 'get-data-team.tenjin_dv_test',
    'merge_columns_list': ['date'],
    },
    {
    'increment_method': 'increment_column',
    'increment_column': 'id',
    'rows_per_increment': 1000000,
    'schema_name': 'tenjin',
    'table_name': 'daily_ad_revenue',
    'bucket': 'datateam_bucket',
    'dataset': 'get-data-team.tenjin_dv_test',
    'merge_columns_list': ['id'],
    }]

"""


table_configs = [ # some atypical tables
{   'table_name': 'campaigns_targeting_tags',
    'increment_method': 'increment_column',
    'increment_column': 'loaded_at',
    'rows_per_increment': 1000*10,
    'schema_name': 'tenjin',
    'bucket': 'datateam_bucket',
    'dataset': 'get-data-team.tenjin_dv_test',
    'merge_columns_list': ['campaign_id', 'targeting_tag_id']
},
{   'table_name': 'reporting_metrics',
    'increment_method': 'increment_column',
    'increment_column': 'loaded_at',
    'rows_per_increment': 1000*10,
    'schema_name': 'tenjin',
    'bucket': 'datateam_bucket',
    'dataset': 'get-data-team.tenjin_dv_test',
    'merge_columns_list': ['date', 'app_id', 'campaign_id', 'country', 'site_id']
},
{   'table_name': 'reporting_cohort_metrics',
    'increment_method': 'increment_column',
    'increment_column': 'loaded_at',
    'rows_per_increment': 1000*10,
    'schema_name': 'tenjin',
    'bucket': 'datateam_bucket',
    'dataset': 'get-data-team.tenjin_dv_test',
    'merge_columns_list': ['install_date', 'days_since_install', 'app_id', 'campaign_id', 'country', 'site_id']
},
{   'table_name': 'countries',
    'increment_method': 'increment_column',
    'increment_column': 'code',
    'rows_per_increment': 1000*10,
    'schema_name': 'tenjin',
    'bucket': 'datateam_bucket',
    'dataset': 'get-data-team.tenjin_dv_test',
    'merge_columns_list': ['code']
},
{   'table_name': 'events',
    'increment_method': 'increment_column',
    'increment_column': 'loaded_at',
    'rows_per_increment': 1000*10,
    'schema_name': 'tenjin',
    'bucket': 'datateam_bucket',
    'dataset': 'get-data-team.tenjin_dv_test',
    'merge_columns_list': ['loaded_at', 'ip_address', 'creative_name', 'uuid', 'app_version', 'purchase_state', 'site_id', 'bundle_id', 'source_uuid', 'developer_device_id', 'product_id', 'quantity', 'event_type', 'acquired_at', 'limit_ad_tracking', 'source_campaign_id', 'country', 'advertising_id', 'device', 'event', 'os_version', 'app_id', 'platform', 'created_at']
},
]


tables_list_column_increment = ['ad_networks', 'apps', 'campaign_buckets', 'campaigns',
'daily_ad_revenue', 'daily_country_spend', 'daily_spend', 'publisher_apps', 'targeting_tags', 'events', 'ad_engagements']

for table in tables_list_column_increment:
    table_configs.append({'table_name': table,
                          'increment_method': 'increment_column',
                          'increment_column': 'loaded_at',
                          'rows_per_increment': 1000*10,
                          'schema_name': 'tenjin',
                          'bucket': 'datateam_bucket',
                          'dataset': 'get-data-team.tenjin_dv_test',
                          'merge_columns_list': ['id']
                         })
