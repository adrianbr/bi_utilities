import redshift_json_download as r
import bq_upload as bq
from datetime import datetime
from google.api_core.exceptions import NotFound
from credential import default_redshift_conn
from google.cloud import bigquery
import os
import re

#suggest creating 2 databases, 1 for the increment tables

def make_bq_table_from_redshift_query(query, local_filename, bucket, filename_on_bucket, dataset, table_name,
                                    date_partition_column=None, redshift_conn=default_redshift_conn):
    """queries redshift and uploads the select result to bigquery
    returns False if there is no data to copy, else returns true.
    """

    #get data locally to json
    file = r.redshift_query_to_file(query, local_filename, redshift_conn=redshift_conn)
    #if file is empty: return and message done
    #else upload it to bq. schema is auto guessed from json (writing avro is slow)
    if os.stat(local_filename).st_size == 0:
        print ("No data to be copied")
        return False
    bq.local_json_to_bq(file, bucket, filename_on_bucket, dataset, table_name, date_partition_column=date_partition_column)
    return True

def full_copy_schema(schema_name='tenjin'):
    query = "select table_name from information_schema.tables where table_schema = '{}' ".format(schema_name)
    tables = r._redshift_select_to_dict(query)
    tables_list = [row['table_name'] for row in tables]
    for table in tables_list:
        #TODO - remove limit
        #query = "select * from {}.{} limit 10000".format(schema_name, table)
        query = "select count(*) from {}.{} limit 10000".format(schema_name, table) # how big are the tables? can it be done in 1 load?
        print(query)
        local_filename = table + '.json'
        bucket = 'datateam_bucket'
        filename_on_bucket = datetime.now().strftime("%Y/%m/%d") + '/{}/{}/{}'.format(schema_name, table, local_filename)
        dataset = 'get-data-team.tenjin_dv_test'
        table_name = table

        make_bq_table_from_redshift_query(query, local_filename, bucket, filename_on_bucket, dataset, table_name,
                                    date_partition_column=None, redshift_conn=default_redshift_conn)



def copy_table_increment_on_column(schema_name, table_name, increment_column, bucket,dataset ,rows_per_increment=1000, increment_prefix=None):
    #check max value for the loaded column

    local_filename = table_config['table_name'] + '.json'
    filename_on_bucket = '{}/{}/{}/{}'.format(schema_name, datetime.now().strftime("%Y/%m/%d"), table_name, local_filename)

    #try to get it from already loaded table, if not get first value from source
    try:
        client = bigquery.Client()
        table_id = "{}.{}".format(dataset, table_name)
        last_val_query = """SELECT MAX({}) FROM {}""".format(increment_column, table_id)
        query_job = client.query(last_val_query)  # Make an API request.
        data = query_job.result()
        rows = list(data)
        # last_val is None if target table doesn't exist yet
        last_val = rows[0][0]

    #but if the table is not found then take it from source start:
    except NotFound as error:
        print("Table not found.")
        last_val_query = """SELECT MIN({}) as inc_val FROM {}""".format(increment_column, schema_name+'.'+table_name)
        last_val_res = r._redshift_select_to_dict(last_val_query)
        print(last_val_res)
        last_val = last_val_res[0]['inc_val']

    print('last_val: ',last_val)
    #make_bq_table_from_redshift_query()


    #now we have the last value so we can load an increment
    #make increment query
    increment_query = """select * 
                        from {schema_name}.{table_name} 
                        where {increment_column} > '{last_value}'
                        order by {increment_column} asc
                        limit {rows_per_increment}
        """.format(schema_name=schema_name,
                   table_name=table_name,
                   increment_column=increment_column,
                   last_value=last_val,
                   rows_per_increment=rows_per_increment
                   )

    #load to an increment table, check if there is still data left to load
    if increment_prefix:
        table_name = increment_prefix + table_name

    data_exists_in_increment = make_bq_table_from_redshift_query(increment_query, local_filename, bucket, filename_on_bucket, dataset, table_name)
    return data_exists_in_increment


def _make_merge_query(increment_table_id, target_table_id, merge_columns_list):
    """merge incremental_load to existing target_table"""
    client = bigquery.Client()
    #try ge target table columns for merge, if not possible then it means it does not exist so just copy instead
    try:
        table = client.get_table(target_table_id)
        columns_list = list(c.name for c in table.schema)
        print(columns_list)
    except:
        return None



    merge_condition = " AND ".join(["t.{col} = i.{col}".format( col = re.sub("[^0-9a-zA-Z]+", "_", colname.lower())) for column in merge_columns_list])

    when_clause_values = ",".join(["t.{col} = i.{col}".format(col=col) for col in columns_list])


    merge_query = """MERGE INTO {target_table_id} as t 
                            USING {increment_table_id} as i 
                            ON {merge_condition}
                            WHEN matched then update 
                            SET {when_clause_values} 
                            WHEN not matched by target THEN insert row 
                            """.format(increment_table_id=increment_table_id,
                                       target_table_id=target_table_id,
                                       merge_condition=merge_condition,
                                       when_clause_values=when_clause_values
                                       )

    print("Merge_query:" , merge_query)
    return merge_query


def copy_table_incrementally_on_column(schema_name, table_name, increment_column, merge_columns_list, bucket, dataset, rows_per_increment=1000, increment_prefix='z_increment_'):
    data_left_to_copy = True
    while data_left_to_copy:
        #copy until there is no more
        #copy to an increment table
        increment_table_id = "{}.{}".format(dataset, increment_prefix + table_name)
        target_table_id = "{}.{}".format(dataset, table_name)
        data_left_to_copy = copy_table_increment_on_column(schema_name, table_name, increment_column, bucket,dataset,
                                                           rows_per_increment=rows_per_increment,
                                                           increment_prefix =increment_prefix)

        #try make merge query, if target table oes not exist then return none, so we know and can copy it instead.
        merge_query = _make_merge_query(increment_table_id, target_table_id, merge_columns_list)
        client = bigquery.Client()
        if merge_query:
            job = client.query(merge_query)  # Make an API request.
        else:
            job = client.copy_table(increment_table_id, target_table_id)
        job.result()  # Wait for the job to complete.
        print("Merged into target table.")


if __name__ == "__main__":
    table_config = {}
    table_config['schema_name'] = 'tenjin'
    table_config['table_name'] = 'daily_ad_revenue'
    table_config['increment_column'] = 'id'
    #table_config['rows_per_increment'] = 1000*1000
    table_config['bucket'] = 'datateam_bucket'
    table_config['dataset'] = 'get-data-team.tenjin_dv_test'
    table_config['merge_columns_list'] = ['id']

    copy_table_incrementally_on_column(**table_config)