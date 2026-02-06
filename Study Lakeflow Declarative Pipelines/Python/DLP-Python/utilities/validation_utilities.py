def get_table_stats(table_schema,table_name):
    df=spark.table(f'{table_schema}.{table_name}')
    print("row count:", df.count(), "column count:", len(df.schema))
    return

def validate_streaming_table(table_schema,table_name):
    sql_query=f"""select table_type from INFORMATION_SCHEMA.TABLES
                 where table_schema= '{table_schema}' and table_name= '{table_name}'"""
    dfTbls=spark.sql(sql_query)
    assert dfTbls.collect()[0]['table_type']=='STREAMING_TABLE',f"Table {table_name} is not a streaming table"
    get_table_stats(table_schema,table_name)
    return

def validate_mv(table_schema,table_name):
    sql_query=f"""select table_type from INFORMATION_SCHEMA.TABLES
                 where table_schema= '{table_schema}' and table_name= '{table_name}'"""
    dfTbls=spark.sql(sql_query)
    assert dfTbls.collect()[0]['table_type']=='MATERIALIZED_VIEW',f"Table {table_name} is not a materialized view"
    get_table_stats(table_schema,table_name)
    return   

def validate_delta(table_schema,table_name):
    sql_query=f"""select table_type from INFORMATION_SCHEMA.TABLES
                 where table_schema= '{table_schema}' and table_name= '{table_name}'"""
    dfTbls=spark.sql(sql_query)
    assert dfTbls.collect()[0]['table_type']=='MANAGED',f"Table {table_name} is not a Delta Lake table"
    get_table_stats(table_schema,table_name)
    return         