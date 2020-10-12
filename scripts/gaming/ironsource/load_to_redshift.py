import utils_redshift
import ironsouce_user_revenue_download_example

secretkey = 'yoursecretkey'
refreshtoken = 'refreshtoken'
appkey = 'appkey'
filename = ironsouce_user_revenue_download_example.download_ironsrc_data(secretkey, refreshtoken, appkey)
# do loading logic
print(filename)

target_table = 'my redshift tablename'

#assumes connections coming from airflow
#copy local file to redshift table via s3
utils_redshift.local_file_to_redshift(filename, target_table, upload_file_name=None, upload_subfolder='',
                           copy_options="format as json 'auto'", redshift_connection='dwh', redshift_schema='dwh',
                           s3_connection='bi_etl_s3')

print("Successfuly loaded to {}!".format(target_table))
