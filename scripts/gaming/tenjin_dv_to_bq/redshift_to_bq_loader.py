import redshift_json_download as r
import bq_upload as bq
from datetime import datetime

from credential import default_redshift_conn

def make_bq_table_from_redshift_query(query, local_filename, bucket, filename_on_bucket, database, table_name,
                                    date_partition_column=None, redshift_conn=default_redshift_conn):
    """queries redshift and uploads the select result to bigquery"""
    #get data locally to json
    file = r.redshift_query_to_file(query, local_filename, redshift_conn=redshift_conn)
    #upload it to bq. schema is auto guessed from json (writing avro is slow)
    bq.local_json_to_bq(file, bucket, filename_on_bucket, database, table_name, date_partition_column=date_partition_column)


def copy_schema(schema_name='tenjin'):
    query = "select table_name from information_schema.tables where table_schema = '{}' ".format(schema_name)
    tables = r._redshift_select_to_dict(query)
    tables_list = [row['table_name'] for row in tables]
    for table in tables_list:
        #TODO - remove limit
        query = "select * from {}.{} limit 10000".format(schema_name, table)
        local_filename = table + '.json'
        bucket = 'test_bucket'
        filename_on_bucket = datetime.now().strftime("%Y/%m/%d") + '/{}/{}/{}'.format(schema_name, table, local_filename)
        database = schema_name
        table_name = table

        make_bq_table_from_redshift_query(query, local_filename, bucket, filename_on_bucket, database, table_name,
                                    date_partition_column=None, redshift_conn=default_redshift_conn)




