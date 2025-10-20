from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr


source_table= f"bronze.{spark.conf.get('source_table')}"
target_table= f"silver.{spark.conf.get('target_table')}"
primary_key=spark.conf.get('primary_key')

dp.create_streaming_table(target_table)

dp.create_auto_cdc_flow(
    source=source_table,
    target=target_table,
    keys=[primary_key],
    sequence_by =col('start_lsn'),
    apply_as_deletes = expr("'__$operation' = 1"),
    except_column_list = ['__$operation','start_lsn','__$start_lsn','__$end_lsn','__$seqval','__$update_mask'],
    stored_as_scd_type = 1
)
