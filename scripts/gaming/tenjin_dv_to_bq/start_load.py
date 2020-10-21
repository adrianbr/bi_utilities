import table_loading_config as c
import redshift_to_bq_loader as l



l.copy_table_configs(c.table_configs)
