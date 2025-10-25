from pyspark import pipelines as dp
import pyspark.sql.functions as F

#Auto CDC from snapshot
# Target table is SCD type 1

dp.create_streaming_table('silver.sales_orders_scd3')
dp.create_auto_cdc_from_snapshot_flow(
    target='silver.sales_orders_scd3',
    source='sales_orders',
    keys=['id'],
    stored_as_scd_type = 1
    )
