import psycopg2
from airflow.hooks.base_hook import BaseHook
import os
import boto3

#assumes credentials are stored in airflow and are retrieved via airflow BaseHook


def query_redshift(connection_id, query):
    ''' query redshift and return rows'''
    myconn = BaseHook.get_connection(connection_id)
    conn = psycopg2.connect(dbname=myconn.schema,
                             user=myconn.login,
                             host=myconn.host,
                             port=myconn.port,
                             password=myconn.password)
    print(db_from)
    cur = conn.cursor()
    print('Executing: \n \n' + query)
    cur.execute(query)
    conn.commit()
    print ('rows affected:' + str(cur.rowcount))
    if cur.description:
        rows = cur.fetchall()
    else:
        rows = None
    cur.close()
    conn.close()
    return rows

def local_file_to_s3(local_file_path, upload_file_name=None, upload_subfolder=None):
    #get creds as dict
    s3conn = {'aws_access_key_id': BaseHook.get_connection('bi_etl_s3').login,
              'aws_secret_access_key': BaseHook.get_connection('bi_etl_s3').password,
              'bucket': BaseHook.get_connection('bi_etl_s3').host
              }

    #get upload filename if not provided
    if upload_file_name is None:
        upload_file_name = os.path.basename(local_file_path)

    #define file path in bucket
    if upload_subfolder is not None:
        upload_path = os.path.join(upload_subfolder, upload_file_name)
    else:
        upload_path = upload_file_name

    #open session
    session = boto3.Session(
        aws_access_key_id = s3conn.get('aws_access_key_id'),
        aws_secret_access_key = s3conn.get('aws_secret_access_key')
    )
    s3 = session.resource('s3')

    #upload file
    s3.meta.client.upload_file(local_file_path, s3conn.get('bucket'), upload_path, ExtraArgs={'ServerSideEncryption': 'AES256'})

    #return path for telling redshift where to pick it up
    return upload_path


def s3_to_redshift(schema, table, file_path, options='CSV', redshift_connection='dwh', s3_connection='bi_etl_s3'):
    #get s3 creds as dict
    s3conn = {'aws_access_key_id': BaseHook.get_connection(s3_connection).login,
              'aws_secret_access_key': BaseHook.get_connection(s3_connection).password,
              'bucket': BaseHook.get_connection(s3_connection).host
              }

    copy_query = """copy {schema}.{table}
                    from 's3://{bucket}/{file_path}'
                    access_key_id '{access_key_id}'
                    secret_access_key '{aws_secret_access_key}'
                    {options};
                 """.format(schema = schema,
                            table = table,
                            bucket = s3conn.get('bucket'),
                            file_path = file_path,
                            access_key_id = s3conn.get('aws_access_key_id'),
                            aws_secret_access_key = s3conn.get('aws_secret_access_key'),
                            options=options
                            )

    query_redshift(redshift_connection, copy_query)

def local_file_to_redshift(local_file_path, table, upload_file_name=None, upload_subfolder='', copy_options="format as json 'auto'", redshift_connection='dwh', redshift_schema = 'dwh', s3_connection='bi_etl_s3'):
    #combine copy to s3 and copy from s3 to redshift
    upload_path = local_file_to_s3(local_file_path, upload_file_name=upload_file_name, upload_subfolder=upload_subfolder)

    s3_to_redshift(redshift_schema, table, upload_path, options=copy_options, redshift_connection=redshift_connection, s3_connection=s3_connection)


def merge_redshift_increment_from_file(tablename, filename, merge_columns = ['date'],
                                       upload_file_name='increment.json',
                                       upload_subfolder='increments',
                                       redshift_connection='dwh',
                                       s3_connection='s3_etl_bucket',
                                       drop_increment_table_on_finish = True):
    #create staging table
    staging_table_ddl = 'CREATE TABLE {schema}.{target_table}_increment ( like {schema}.{target_table} )'.format(target_table = tablename)

    #load data to staging table

    #merge
    merge_columns_sql = ' \n'.join(["and {tablename}.{col} = {tablename}_increment.{col}".format(tablename=tablename, col=col) for col in merge_columns])
    merge_table_sql = '''DELETE FROM {schema}.{tablename} USING {schema}.{tablename}_increment
                        WHERE 1=1
                        {merge_columns_sql}
                        ;
                        INSERT INTO {schema}.{tablename} select * from {schema}.{tablename}_increment
                        '''.format(tablename=tablename, schema=redshift_schema, merge_columns_sql=merge_columns_sql)
    drop_staging_table_sql = 'drop table if exists {schema}.{tablename}_increment'.format(schema=redshift_schema, tablename=tablename)


    #drop staging table if exists
    query_redshift(redshift_connection, drop_staging_table_sql)
    #create new staging tabl
    query_redshift(redshift_connection, staging_table_ddl)

    #load data to staging table
    local_file_to_redshift(filename, redshift_schema, tablename + '_increment',
                           upload_file_name=upload_file_name,
                           upload_subfolder=upload_subfolder,
                           options=copy_options,
                           redshift_connection=redshift_connection,
                           s3_connection=s3_connection)

    #merge staging increment with target
    query_redshift(redshift_connection, merge_table_sql)
    #drop increment table
    if drop_increment_table_on_finish is True:
        query_redshift(redshift_connection, drop_staging_table_sql)
    print ('merged {0} into {1} on {2}'.format(filename, tablename, ', '.join(merge_columns)))


