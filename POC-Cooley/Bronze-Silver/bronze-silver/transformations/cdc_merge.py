from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr
import json

# table_list=json.loads(spark.conf.get('table_list'))

def ingest_cdc_into_silver(p_source_table,p_target_table,primary_key):
    source_table_full_name= f"bronze.{p_source_table}"
    target_table_full_name= f"silver.{p_target_table}"

    dp.create_streaming_table(target_table_full_name)
    dp.create_auto_cdc_flow(
        source=source_table_full_name,
        target=target_table_full_name,
        keys=[primary_key],
        sequence_by =col('start_lsn'),
        apply_as_deletes = expr("'__$operation' = 1"),
        except_column_list = ['__$operation','start_lsn','__$start_lsn','__$end_lsn',
                              '__$seqval','__$update_mask','__$command_id'],
        stored_as_scd_type = 1
    )

table_list=[{"source_table":"salesorderdetail","target_table":"salesorderdetail","primary_key":"SalesOrderDetailID"}, {"source_table":"salesorderheader","target_table":"salesorderheader","primary_key":"SalesorderID"}]

for table in table_list:
    ingest_cdc_into_silver(table['source_table'],\
        table['target_table'],table['primary_key'])