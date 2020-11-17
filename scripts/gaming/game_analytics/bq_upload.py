from google.cloud import bigquery
from google.cloud import storage
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="creds.json"



def _cloud_storage_upload(local_file, bucket, filename_on_bucket):
    """uploads file to Google Cloud storage"""
    client = storage.Client()

    bucket = client.get_bucket(bucket)
    blob = bucket.blob(filename_on_bucket)
    blob.upload_from_filename(local_file)
    print('uploaded ', bucket, filename_on_bucket)


def _cloud_storage_to_bq(bucket, filename_on_bucket, dataset, table_name, date_partition_column=None):

    client = bigquery.Client()
    table_id = "{}.{}".format(dataset, table_name)

    if date_partition_column is not None:
        partition_dict = {'object_type': bigquery.table.TimePartitioning(date_partition_column),
                          'field': date_partition_column
        }
    else:
        partition_dict = {}

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect = True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, #WRITE_APPEND,
        **partition_dict
    )

    print(job_config)

    uri = "gs://{}/{}".format(bucket, filename_on_bucket)
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # API request

    load_job.result()  # Waits for the job to complete.
    destination_table = client.get_table(table_id)
    print("Loaded {} rows. to {}".format(destination_table.num_rows, table_id))


def _make_merge_query(increment_table_id, target_table_id):
    """merge incremental_load to existing target_table"""
    client = bigquery.Client()
    #try ge target table columns for merge, if not possible then it means it does not exist so just copy instead
    try:
        table = client.get_table(target_table_id)
        columns_list = list(c.name for c in table.schema)
        columns = ','.join(columns_list)
        #print(columns_list)
    except:
        return None


    merge_query = f"""
    delete from `{target_table_id}`
        where date in 
            (select distinct date from `{increment_table_id}`);
    
    insert into `{target_table_id}` ({columns})
        select {columns} from `{increment_table_id}`;
    """

    print("Merge_query:" , merge_query)
    return merge_query

def merge_bq_tables(increment_table_id, target_table_id, drop_increment_after_finish=True):
    """merge tables on merge columns"""
    merge_query = _make_merge_query(increment_table_id, target_table_id)
    client = bigquery.Client()

    if merge_query:
        job = client.query(merge_query)  # Make an API request.
    else:
        job = client.copy_table(increment_table_id, target_table_id)
    job.result()  # Wait for the job to complete.
    print("Merged into target table.")

    if drop_increment_after_finish:
        client = bigquery.Client()
        job = client.delete_table(increment_table_id, not_found_ok=True)
        print("dropped increment table.")

def local_metrics_file_to_bq(local_file, bucket, filename_on_bucket, dataset, table_name, date_partition_column='date'):
    increment_tablename = 'z_temporary_increment_' + table_name

    _cloud_storage_upload(local_file, bucket, filename_on_bucket)


    _cloud_storage_to_bq(bucket, filename_on_bucket, dataset, increment_tablename, date_partition_column=date_partition_column)
    increment_table_id = f"{dataset}.{increment_tablename}"
    table_id = f"{dataset}.{table_name}"

    merge_bq_tables(increment_table_id, table_id)


