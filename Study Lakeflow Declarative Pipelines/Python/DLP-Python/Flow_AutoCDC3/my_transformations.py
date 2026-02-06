from pyspark import pipelines as dp
import pyspark.sql.functions as F

#Auto CDC from snapshot
# Target table is SCD type 1

def sales_orders_incremental_read(ingestion_version):
  increment_version = lambda ingestion_version: ingestion_version + 1 if ingestion_version is not None else 1
  new_ingestion_version = increment_version(ingestion_version)     
  df=spark.read.table('bronze.sales_orders_history')\
      .filter(F.col('ingestion_version') == new_ingestion_version)  
  if df.count()>0:
     return (df,new_ingestion_version)
  else:
    return None

dp.create_streaming_table('silver.sales_orders_scd3')
dp.create_auto_cdc_from_snapshot_flow(
    target='silver.sales_orders_scd3',
    source=sales_orders_incremental_read,
    keys=['id'],
    stored_as_scd_type = 1
    )




