from pyspark import pipelines as dp
import pyspark.sql.functions as F

# Target table is SCD type 2

dp.create_streaming_table('silver.sales_orders_scd2')
dp.create_auto_cdc_flow(flow_name='sales_orders_scd2',
    target='silver.sales_orders_scd2',
    source='sales_orders_cdf',
    keys=['id'],
    sequence_by = F.col("_commit_version"),
    apply_as_deletes = F.expr("_change_type = 'delete'"),
    except_column_list = ["_change_type", "_commit_version","_commit_timestamp"],
    stored_as_scd_type = 2,
    track_history_column_list = ["order_status"]
    # track_history_except_column_list =["order_date"]
    )
