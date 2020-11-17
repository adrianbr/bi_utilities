from bq_upload import local_metrics_file_to_bq
import ga_reporting_api_downloader




files = ga_reporting_api_downloader.download_data_and_make_files()
for file in files:
    bucket = 'datateam_bucket'
    filename_on_bucket = file
    dataset = 'get-data-team.tenjin_dv_test'
    table_name = file.replace('.json','')


    local_metrics_file_to_bq(file, bucket, filename_on_bucket, dataset, table_name)

    print(f"Successfuly loaded to {dataset}.{table_name}!")
