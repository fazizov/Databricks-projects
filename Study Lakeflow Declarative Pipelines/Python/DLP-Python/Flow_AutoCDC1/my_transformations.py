from pyspark import pipelines as dp
import pyspark.sql.functions as F

dp.create_streaming_table('silver.sales_orders_cleansed')
dp.create_auto_cdc_flow(flow_name='sales_orders',
    target='silver.sales_orders_cleansed',
    source='sales_orders_cdf',
    keys=['id'],
    sequence_by = F.col("_commit_version"),
    apply_as_deletes = F.expr("_change_type = 'delete'"),
    except_column_list = ["_change_type", "_commit_version","_commit_timestamp"],
    stored_as_scd_type = 1
    )
