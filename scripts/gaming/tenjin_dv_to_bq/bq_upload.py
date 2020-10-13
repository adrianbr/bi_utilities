from google.cloud import bigquery
from google.cloud import storage
import os


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service_account_secret.json"


def _cloud_storage_upload(local_file, bucket, filename_on_bucket):
    """uploads file to Google Cloud storage"""
    client = storage.Client()

    bucket = client.get_bucket(bucket)
    blob = bucket.blob(filename_on_bucket)
    blob.upload_from_filename(local_file)
    print('uploaded ', bucket, filename_on_bucket)


def _cloud_storage_to_bq(bucket, filename_on_bucket, database, table_name, date_partition_column=None):


    client = bigquery.Client()
    table_id = "acrm-analytics-poc.data_exploration_increment.{}_{}".format(database, table_name)

    if date_partition_column is not None:
        partition_dict = {'object_type': bigquery.table.TimePartitioning(date_partition_column),
                          'field': date_partition_column
        }

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # WRITE_TRUNCATE
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


def local_json_to_bq(local_file, bucket, filename_on_bucket, database, table_name, date_partition_column=None):

    _cloud_storage_upload(local_file, bucket, filename_on_bucket)

    _cloud_storage_to_bq(bucket, filename_on_bucket, database, table_name, date_partition_column=date_partition_column)