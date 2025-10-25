from pyspark import pipelines as dp
import pyspark.sql.functions as F

#Auto CDC fron snapshot
# Target table is SCD type 2
def sales_orders_incremental_read(ingestion_version):
  if ingestion_version is None:
    new_ingestion_version=1
  else:      
    new_ingestion_version=ingestion_version+1
  df=spark.read.table('bronze.sales_orders_history')\
      .filter(F.col('ingestion_version') == new_ingestion_version)  
  if df.count()>0:
     return (df,new_ingestion_version)
  else:
    return None

dp.create_streaming_table('silver.sales_orders_snapshot_scd2')
dp.create_auto_cdc_from_snapshot_flow(
    target='silver.sales_orders_snapshot_scd2',
    source=sales_orders_incremental_read,
    keys=['id'],
    stored_as_scd_type = 2,
    track_history_column_list = ["order_status"]
    # track_history_except_column_list =["order_date"]
    )
