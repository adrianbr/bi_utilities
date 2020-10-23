# Tenjin Redshift to BQ loader

This is a database copier designed for grabbing data from redshift and pushing it to bigquery without using s3.
 
 
The reason for this is that a customer on gcp that wants to consume data from a 3rd party redshift should not have to worry about getting an aws account just for s3.

The copier can handle 3 types of increment scenarios:  
1. No increment (full load)
2. Increment on column last value + merge on PK columns
3. Take a custom query for increment (such as last 30d or even a complex select) and merge it on PK columns.

# How it works
##For the last value based copy:
1. Last value is read from the table in BQ, if table does not exist, then we take the first (min) value from redshift for the table
2. An increment of configurable size ('rows_per_increment' = 1000000 ) is copied to bigquery into an increment table
- data is 'selected' into the python environment
- json file with data is written
- json file is sent to google storage bucket (`filename_on_bucket = '{}/{}/{}/{}'.format(schema_name, datetime.now().strftime("%Y/%m/%d"), table_name, local_filename)`)
- json file is read into BQ (autodetect schema) into an increment table (named z_increment_tablename so it sorts to the bottom)
3. The increment table is merged to the target BQ table on the merge columns (merge operation happens as explained by AWS, an "upsert" on PK. The BQ merge gives other options too).

##For the query based copy all things are the same except 
1. An increment is selected based on a custom query passed into the config.`

##possible improvements:
- for the tables that are loaded based on a query from tenjin dv, first check the updated_at table to see if there is anything new to load.



#necessary resources:
1. Python environment to run the code. Since it copies large files, a small cloud function might not suffice. The job does not require significant compute, but needs ram and disk to fit the data slices. The only way to avoid transfer through the local environment is to use the specialised google service 
https://cloud.google.com/storage-transfer/docs/create-manage-transfer-program

2. Google storage bucket (or multiple, a different one can be passed per table)

3. Google BQ to load to


##Credentials
For gcp:
https://cloud.google.com/docs/authentication/getting-started
you must make sure to give the following permissions in IAM
` BigQuery Admin,
Storage Admin,
Storage Object Admin`

You might be able to restrict them more if need be.

For Redshift:
Edit the `credential.py` file

## Configuring the copy
`table_load_config.py` contains the table config. If you wish to use a custom loading strategy or a query-based lookback, 
you can edit this config according to the examples from the config file

This config is a list of parameter dictionaries based on which the copy is performed, 
such as the query or tablename, increment columns, bucket to use, etc

##starting the load
Once credentials and config are finalised, start the load by simply running in your terminal
`python3 start_load.py`
in the folder with the script. (use the terminal command `pwd to confirm you are in the right folder)
