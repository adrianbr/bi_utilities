#load ironsource metrics to bq
current script requests lasts 3 days of data and replaces them in the target table


here we take 3 days under the assumption that the data needs some time to be finalized

target table or download window can be configured in the script load_ironsource_metrics_to_bq.py

#authentication
a google service account ``creds.json ``file is required for authentication 
and the ironsource api credentials which are configurable in the entry script

#start loading
to start loading, run ``python load_ironsource_metrics_to_bq.py``