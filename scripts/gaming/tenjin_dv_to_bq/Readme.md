# Summary: Tenjin DataVault (Redshift) to BigQuery loader

This is a database copier designed for grabbing data from Redshift and pushing it to BigQuery without using AWS S3.  

The copier can handle 3 types of increment scenarios:  
1. No increment (full load)
2. Increment on column last value + merge on PK columns
3. Take a custom query for increment (such as last 30d or even a complex select) and merge it on PK columns.

Ultimately, all methods run a query and retrieve data which is then saved as a table (and merged to a target).

# Summary: Value to Tenjin DataVault clients

- Cost savings
Typically Tenjin clients who use BigQuery for data warehousing use this [advice](https://help.tenjin.io/t/how-to-use-datavault-with-google-bigquery/6059)
"Increment scenarios", simplified, mean that you only look for data that has changed. You do not do full unloads any longer and you do not need to use/pay for AWS S3 or other tools. 

- Less dependency in ETL on Tenjin DataVault updates, less overhead for engineers, fresher data
Typically clients start using DataVault by following this [ETL advice](https://help.tenjin.io/t/datavault-updates-schedule/3528) and trigger their ETL job around 7AM UTC and then later at 1PM UTC. Data engineers have to monitor when an ETL has finished. With this database copier clients become less reliant on Tenjin's update schedule. They can run the code, say, 12 times a day. 

# How-to: Why the above 3 scenarios? 

1. The **full load** is not exposed to the config but you can call it directly from redshift_to_bq_loader.full_copy_schema
the use case is to copy the data once entirely for an ad hoc analysis or for incrementing on it later.

The data goes from Redshift via select -> Local file (json) -> to gogole storage -> Bigquery target table

2. The **increment on column value** checks the loaded table (if exists) and only takes the new rows. 
It then merges them though an upsert-like transaction. This way we are economical about copy time and resources.

The data goes from Redshift via select -> Local file (json) -> to gogole storage -> Bigquery increment table -> (IDEMPOTENT MERGE) Bigquery target table

3. Because some tables are **better loaded by custom rules** (such as last 30d, or any other filters) or because you might want to copy only some columns, or your own aggregated metric resulting from a query, we can use this method that takes a query output and treats it as a table (as for point 2). The data flow is the same as for point 3.

Which scenario you should choose depends on your business case such as the channels you use for UA or how advanced your bidding strategy is. Please
talk to your Tenjin account manager for advice if needed.

# How-to
### Configuring the copy
`table_load_config.py` contains the table config. If you wish to use a custom loading strategy or a query-based lookback, 
you can edit this config according to the examples from the config file

This config is a list of parameter dictionaries based on which the copy is performed, 
such as the query or tablename, increment columns, bucket to use, etc

### Starting the load
Once credentials and config are finalised, start the load by simply running in your terminal
`python3 start_load.py`
in the folder with the script. (use the terminal command `pwd to confirm you are in the right folder)

### For the last value based copy
1. Last value is read from the table in BigQuery, if table does not exist, then we take the first (min) value from Redshift for the table
2. An increment of configurable size ('rows_per_increment' = 1000000 ) is copied to BigQuery into an increment table
- data is 'selected' into the python environment
- json file with data is written
- json file is sent to google storage bucket (`filename_on_bucket = '{}/{}/{}/{}'.format(schema_name, datetime.now().strftime("%Y/%m/%d"), table_name, local_filename)`)
- json file is read into BigQuery (autodetect schema) into an increment table (named z_increment_tablename so it sorts to the bottom)
3. The increment table is merged to the target BigQuery table on the merge columns (merge operation happens as explained by AWS, an "upsert" on PK. The BigQuery merge gives other options too).

### For the query based copy all things are the same except 
1. An increment is selected based on a custom query passed into the config.`

### Possible improvements
- for the tables that are loaded based on a query from Tenjin DataVault, first check the updated_at table to see if there is anything new to load.

# Setup

### Requirements

1. Python environment to run the code. Since it copies large files, a small cloud function might not suffice. The job does not require significant compute, but needs ram and disk to fit the data slices. The only way to avoid transfer through the local environment is to use the specialised google service 
https://cloud.google.com/storage-transfer/docs/create-manage-transfer-program

2. Google storage bucket (or multiple, a different one can be passed per table)

3. Google BigQuery to load to

### Credentials

For GCP:
https://cloud.google.com/docs/authentication/getting-started
you must make sure to give the following permissions in IAM
` BigQuery Admin,
Storage Admin,
Storage Object Admin`

You might be able to restrict them more if need be.

For Redshift:
Edit the `credential.py` file
