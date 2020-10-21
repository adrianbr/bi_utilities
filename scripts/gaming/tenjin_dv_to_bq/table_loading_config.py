#the table configs are stored in a list of dicts.

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


"""


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
