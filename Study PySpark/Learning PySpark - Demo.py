# Databricks notebook source
# MAGIC %md
# MAGIC # Dataframe read/write APIs

# COMMAND ----------

# MAGIC %md
# MAGIC ## File system commands (https://docs.databricks.com/en/files/index.html)

# COMMAND ----------

rootFolder="dbfs:/FileStore"
tablePath='hive_metastore.default'

# COMMAND ----------

# DBTITLE 1,File system commands
# MAGIC %fs
# MAGIC ls dbfs:/FileStore

# COMMAND ----------

# DBTITLE 1,Using dbutils
dbutils.fs.ls(rootFolder)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read/write structured flat files

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading into dataframe

# COMMAND ----------

# DBTITLE 1,Implicit schema reads
dfs=spark.read.format('csv').load(f'{rootFolder}/sales_data.csv')
display(dfs)

# COMMAND ----------

# DBTITLE 1,Inspecting dataframe schema
dfs.printSchema

# COMMAND ----------

# MAGIC %md
# MAGIC **Using common source-specific options:**
# MAGIC - Infer schema
# MAGIC - Column headers
# MAGIC - Column delimiter 

# COMMAND ----------

# DBTITLE 1,Using common source-specific options
dfs=spark.read.format('csv')\
  .option('header','true')\
  .option('inferSchema','true')\
  .option('delimiter',',')\
  .load(f'{rootFolder}/sales_data.csv')
display(dfs)

# COMMAND ----------

# DBTITLE 1,Specifying explicit schema
csv_schema = "product_name string,sales_date date,client_name string, price double, quantity int, sales_amount float"
dfs=spark.read.format('csv')\
  .option('header','true')\
  .schema(csv_schema)\
  .load(f'{rootFolder}/sales_data.csv')
display(dfs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read/Write Delta tables

# COMMAND ----------

# DBTITLE 1,Writing to Delta tables: append mode
tablePath='hive_metastore.default'
dfs.write.format('delta').mode('append').saveAsTable(f'{tablePath}.sales_bronze')

# COMMAND ----------

# DBTITLE 1,Using save method
dfs.write.format('delta').mode('append').save('dbfs:/user/hive/warehouse/sales_bronze2')

# COMMAND ----------

# DBTITLE 1,Reading from Delta tables
dfs=spark.table(f'{tablePath}.sales_bronze')
display(dfs)

# COMMAND ----------

# DBTITLE 1,Executing SQL queries
dfs=spark.sql(f'Select * from {tablePath}.sales_bronze')
display(dfs)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.default.sales_bronze

# COMMAND ----------

# DBTITLE 1,Querying table metadata
display(spark.sql(f'DESCRIBE TABLE EXTENDED {tablePath}.sales_bronze'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read/write semi-structured files

# COMMAND ----------

rootFolder="dbfs:/FileStore"
tablePath='hive_metastore.default'

# COMMAND ----------

# DBTITLE 1,Exploring file content
dbutils.fs.head(f'{rootFolder}/product_data2.json')

# COMMAND ----------

# DBTITLE 1,Schema inference
dfp=spark.read.format('json')\
    .option("multiLine", True)\
    .option("inferSchema", True)\
    .load(f'{rootFolder}/product_data2.json')
display(dfp)

# COMMAND ----------

# DBTITLE 1,Providing explicit schema
from pyspark.sql.types import *
json_schema = StructType([StructField("product_name", StringType(), True),\
    StructField("color", StringType(), True),
    StructField("category", 
                StructType([StructField("furniture_type", StringType(), True),
                           StructField("room_type", StringType(), True)]), True)
    ]                         )        
dfp=spark.read.format('json')\
    .option("multiLine", True)\
    .schema(json_schema)\
    .load(f'{rootFolder}/product_data2.json')

display(dfp)

# COMMAND ----------

# DBTITLE 1,Writing to Delta tables: overwrite mode
dfp.write.format('delta').mode('overwrite').saveAsTable(f'{tablePath}.product_bronze')    

# COMMAND ----------

# MAGIC %md
# MAGIC # Column transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ###Selecting subset of columns

# COMMAND ----------

rootFolder="dbfs:/FileStore"
tablePath='hive_metastore.default'

dfs=spark.read.format('csv')\
  .option('header','true')\
  .option('inferSchema','true')\
  .option('delimiter',',')\
  .load(f'{rootFolder}/sales_data.csv')

# COMMAND ----------

# DBTITLE 1,select method
from pyspark.sql.functions import col
dfs1=dfs.select('product_name','sales_date','client_name',col('sales_amount').alias('SalesAmount'))
display(dfs1)

# COMMAND ----------

# DBTITLE 1,selectExpr method
dfs2 = dfs.selectExpr("product_name as ProductName",
                      "sales_date as SalesDate",
                      "client_name as ClientName",
                      "cast(price as decimal(10,2))as Price",
                      "quantity as Quantity",
                      "cast(sales_amount as decimal(10,2)) as SalesAmount", 
                      "round(sales_amount * 1.13,2) as TaxesAmount",
                      "_metadata['file_name'] as SourceFileName")    
display(dfs2)

# COMMAND ----------

# DBTITLE 1,Using drop method
display(dfs.drop("client_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Calculated columns

# COMMAND ----------

rootFolder="dbfs:/FileStore"
tablePath='hive_metastore.default'

# COMMAND ----------

# DBTITLE 1,Using withColumn with sql functions
from pyspark.sql.functions import round,cast,col,lit,input_file_name,monotonically_increasing_id,when
dfs3=dfs.withColumn('TaxesAmount', round(dfs.sales_amount * 1.13))\
    .withColumn('SalesAmount', col('sales_amount').cast('decimal(10,2)'))\
    .withColumn('FixedStr', lit('Fixed'))\
    .withColumn('RowID', monotonically_increasing_id())\
    .withColumn('product_category', when(col('product_name') == 'Bed', 'Bedroom').otherwise('LivingRoom'))    
display(dfs3)

# COMMAND ----------

dfp=spark.table(f'{tablePath}.product_bronze')
display(dfp)

# COMMAND ----------

# DBTITLE 1,Parsing semi-structured data
dfps=dfp.selectExpr("product_name", "color","category.furniture_type as furniture_type",
        "category.room_type as room_type").withColumn('product_id', monotonically_increasing_id())
dfps.write.format('delta').mode('overwrite').saveAsTable(f'{tablePath}.product_silver')        
display(dfps)                    

# COMMAND ----------

# MAGIC %md
# MAGIC # Table transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filters

# COMMAND ----------

tablePath='hive_metastore.default'
dfs=spark.table(f'{tablePath}.sales_bronze')

# COMMAND ----------

# DBTITLE 1,Using filter/where methods
dfs4=dfs.filter("product_name = 'Bed' AND price > 100")
# dfs4=dfs.where('product_name = "Bed"')
display(dfs4)

# COMMAND ----------

# MAGIC %md
# MAGIC **Common filtering conditions**

# COMMAND ----------

# DBTITLE 1,Null checks
dfs4=dfs.where(col('product_name').isNotNull())
display(dfs4)

# COMMAND ----------

# DBTITLE 1,IN clause condition
dfs4=dfs.where(dfs.product_name.isin("Bed", "Chair"))
display(dfs4)

# COMMAND ----------

# DBTITLE 1,Like condition
dfs4=dfs.where(dfs.product_name.like("Be%"))
display(dfs4)

# COMMAND ----------

# DBTITLE 1,Exclusion condition
dfs4=dfs.where(~dfs.product_name.isin("Bed", "Chair"))
display(dfs4)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ordering/sorting results

# COMMAND ----------

# DBTITLE 1,Ordering in descending order
display(dfs.orderBy(['product_name','sales_amount'], ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table Joins

# COMMAND ----------

tablePath='hive_metastore.default'
dfp=spark.table(f'{tablePath}.product_silver')
dfs=spark.table(f'{tablePath}.sales_bronze')

# COMMAND ----------

# DBTITLE 1,Joins on common column name
dfsp=dfs.join(dfp,"product_name","left")\
    .select(dfs.sales_date,dfs.client_name,dfs.price,dfs.quantity,dfs.sales_amount,
            dfs.product_name,dfp.room_type,dfp.color)
display(dfsp)

# COMMAND ----------

# DBTITLE 1,Alternative join condition
dfsp=dfs.join(dfp,dfs.product_name==dfp.product_name)\
  .select(dfs.sales_date,dfs.client_name,dfs.price,dfs.quantity,
          dfs.sales_amount,dfs.product_name,dfp.room_type,dfp.color)
display(dfsp)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregates

# COMMAND ----------

# DBTITLE 1,Aggregates on entire table
tablePath='hive_metastore.default'
dfs=spark.table(f'{tablePath}.sales_bronze')
print(dfs.count())

# COMMAND ----------

# DBTITLE 1,Multiple aggregates
from pyspark.sql.functions import *
dfsa=dfsp.agg(countDistinct("product_name").alias("DistinctProducts")\
  ,sum("sales_amount").alias("TotalSales"),sum("quantity").alias("TotalQuantity"))
display(dfsa)

# COMMAND ----------

# DBTITLE 1,Partial aggregates-single aggregate
display(dfs.groupBy("product_name").sum("sales_amount"))

# COMMAND ----------

# DBTITLE 1,Partial aggregates-multiple aggregates
dfsa=dfsp.groupBy("product_name").agg(sum("sales_amount").alias("TotalSales"),sum("quantity").alias("TotalQuantity"))
display(dfsa)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Windowing functions

# COMMAND ----------

tablePath='hive_metastore.default'
dfs=spark.table(f'{tablePath}.sales_bronze')

# COMMAND ----------

# DBTITLE 1,Row numbering
from pyspark.sql import Window
from pyspark.sql.functions import row_number
dfspw=dfs.withColumn("row_id",row_number().over(Window.partitionBy("product_name").orderBy("sales_amount")))
display(dfspw)

# COMMAND ----------

# DBTITLE 1,Lag/Lead
from pyspark.sql import Window
from pyspark.sql.functions import row_number
dfspw=dfs.select("product_name","sales_date","sales_amount")\
    .withColumn("row_id",row_number().over(Window.partitionBy("product_name").orderBy("sales_amount")))\
    .withColumn("previous_sale",lag('sales_amount',1).over(Window.partitionBy("product_name").orderBy("sales_date")))\
    .withColumn("next_sale",lead('sales_amount',1).over(Window.partitionBy("product_name").orderBy("sales_date")))

display(dfspw)

# COMMAND ----------

# MAGIC %md
# MAGIC # Temporary/Global views

# COMMAND ----------

dfs=spark.table(f'{tablePath}.sales_bronze')

# COMMAND ----------

# DBTITLE 1,Create temporary views
dfs.createOrReplaceTempView("dfs_temp")
display(sql("SELECT * FROM dfs_temp"))

# COMMAND ----------

# DBTITLE 1,Create global views
dfs.createOrReplaceGlobalTempView("dfs_global_view")

# COMMAND ----------

# MAGIC %md
# MAGIC # Functions

# COMMAND ----------

tablePath='hive_metastore.default'
dfs=spark.table(f'{tablePath}.sales_bronze')

# COMMAND ----------

# DBTITLE 1,Dataframe- level functions
def ingest_data(p_format,p_path):
    return spark.read.format(p_format).option('inferSchema','true').load(p_path)
df=ingest_data("csv",f'{rootFolder}/sales_data.csv') 
display(df)   

# COMMAND ----------

# DBTITLE 1,Python scalar UDFs
@udf
def tax_udf1(p_price,p_quantity):
    return p_price*p_quantity*0.13
display(dfs.withColumn("TaxesAmount",tax_udf1(col("price"),col("quantity"))))

# COMMAND ----------

@udf(returnType="float",useArrow=True)
def tax_udf2(p_price,p_quantity):
    return p_price*p_quantity*0.13
display(dfs.withColumn("TaxesAmount",tax_udf2(col("price"),col("quantity"))))

# COMMAND ----------

# MAGIC %md
# MAGIC Optimized arrow UDFs (see https://docs.databricks.com/en/udf/pandas.html for more details)

# COMMAND ----------

# DBTITLE 1,Pandas UDF: Series to series
from pyspark.sql.functions import pandas_udf,col
import pandas as pd

@pandas_udf("float")
def tax_udf3(p_price:pd.Series,p_quantity:pd.Series)->pd.Series:
    return p_price*p_quantity*0.13
display(dfs.withColumn("TaxesAmount",tax_udf3(col("price"),col("quantity"))))

# COMMAND ----------

# DBTITLE 1,Pandas UDF: Iterator of Series to Iterator of Series
from pyspark.sql.functions import pandas_udf,col
import pandas as pd
from typing import Iterator

@pandas_udf("float")
def tax_udf4(p_price:Iterator[pd.Series])->Iterator[pd.Series]:
    for p in p_price:
        yield p*0.13
    
display(dfs.withColumn("TaxesAmount",tax_udf4(col("price"))))

# COMMAND ----------

# DBTITLE 1,Pandas UDF: Multiple Iterators of Series to Iterator of Series
from pyspark.sql.functions import pandas_udf,col
import pandas as pd
from typing import Iterator,Tuple

@pandas_udf("float","int")
def tax_udf4(iter_var:Iterator[Tuple[pd.Series,pd.Series]])->Iterator[pd.Series]:
    for price,quantity in iter_var:
        yield price*quantity*0.13
    
display(dfs.withColumn("TaxesAmount",tax_udf4("price","quantity")))

# COMMAND ----------

# DBTITLE 1,Custom aggregation using ApplyInPandas
import pandas as pd
def custom_aggregator(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby('product_name', as_index=False).agg({'client_name':lambda x: list(set(x))})
dfsm=dfs.groupBy('product_name')\
    .applyInPandas(custom_aggregator,schema="product_name string, client_name array<string>")
display(dfsm)    

# COMMAND ----------

# DBTITLE 1,Custom parser using mapInPandas
import pandas as pd
from typing import Iterator
def custom_parser(iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for row in iter:
        yield row.explode('client_name')    
    
dfscp=dfsm.mapInPandas(custom_parser,schema="product_name string, client_name string")
display(dfscp)    

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Schema evolution

# COMMAND ----------

tablePath='hive_metastore.default'
dfp=spark.table(f'{tablePath}.product_silver')
dfp.select('product_name').write.mode('append').saveAsTable(f'{tablePath}.product_silver2')

# COMMAND ----------

# DBTITLE 1,Schema mismatch error
dfp.select('product_name','color','furniture_type','room_type')\
    .write.mode('append')\
    .saveAsTable(f'{tablePath}.product_silver2')

# COMMAND ----------

# DBTITLE 1,Schema evolution option
dfp.select('product_name','color','furniture_type','room_type')\
    .write.mode('append')\
    .option("mergeSchema", "true")\
    .saveAsTable(f'{tablePath}.product_silver2')

display(spark.table(f'{tablePath}.product_silver2'))

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table product_silver1

# COMMAND ----------

# MAGIC %md
# MAGIC # Finer table structure commands

# COMMAND ----------

dfp=spark.table(f'{tablePath}.product_silver').drop("product_id")

# COMMAND ----------

# DBTITLE 1,DDL command with auto id and calculated column
sqlCmd=""" CREATE OR REPLACE TABLE  hive_metastore.default.product_silver_ddl (
              product_id BIGINT GENERATED BY DEFAULT AS IDENTITY,
              product_name STRING, 
              room_type STRING,
              furniture_type STRING,
              product_category STRING GENERATED ALWAYS AS (concat(room_type,'/' ,furniture_type))) """
spark.sql(sqlCmd)

# COMMAND ----------

dfp.write.mode('overwrite')\
    .option("mergeSchema", "true")\
    .saveAsTable(f'{tablePath}.product_silver_ddl')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from product_silver_ddl

# COMMAND ----------

# MAGIC %md
# MAGIC # Using Delta APIs for advanced ingestion logic
# MAGIC (See https://docs.delta.io/latest/api/python/spark/index.html for more details)

# COMMAND ----------

rootFolder="dbfs:/FileStore"
tablePath='hive_metastore.default'

# COMMAND ----------

# DBTITLE 1,Creating source dataframe
srcdf = spark.read.format('csv')\
  .option('header','true')\
  .option('inferSchema','true')\
  .option('delimiter',',')\
  .load(f'{rootFolder}/product_data.json')

# COMMAND ----------

# DBTITLE 1,Read data
from delta.tables import *
deltaTableTrg = DeltaTable.forPath(spark, 'dbfs:/user/hive/warehouse/product_silver')
deltaTableTrg.toDF().display()

# COMMAND ----------

# DBTITLE 1,Updating table
deltaTableTrg.update(condition="product_name='Chair'",set={"color": "'White'"})

# COMMAND ----------

# DBTITLE 1,Delete rows
deltaTableTrg.delete(condition="color='White'")

# COMMAND ----------

# DBTITLE 1,Simple upsert logic
deltaTableTrg.alias('T').merge(
    source=srcdf.alias('S'),condition="T.product_name=S.product_name")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()
        


# COMMAND ----------

# DBTITLE 1,Controlled upsert logic
deltaTableTrg.alias('T').merge(
    source=srcdf.alias('S'),condition="T.product_name=S.product_name")\
    .whenMatchedUpdate(set={"color": "S.color","category": "S.category"})\
    .whenNotMatchedInsert(values={"color": "S.color","category": "S.category"})
    # .whenMatchedDelete(condition=S.DeleteFlag==1)
                          

# COMMAND ----------

# DBTITLE 1,Optimize  table 's file structure
deltaTableTrg.optimize()

# COMMAND ----------

# MAGIC %md
# MAGIC # Common data engineering challenges and solutions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deduplication

# COMMAND ----------

dfs=spark.table(f'{tablePath}.sales_bronze')

# COMMAND ----------

# DBTITLE 1,Data deduplication
dfsd=dfs.distinct()
print (dfsd.count(),dfs.count())

# COMMAND ----------

# DBTITLE 1,Deduplicate on subset of columns
dfsd=dfs.dropDuplicates(['product_name','client_name'])
print (dfsd.count(),dfs.count())

# COMMAND ----------

# DBTITLE 1,Deduplicate by window function
from pyspark.sql.functions import desc

dfd=dfs.withColumn("row_id",row_number().over(Window.partitionBy("product_name").orderBy(desc("sales_amount"))))\
    .where("row_id==1")
display(dfd)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling missing values

# COMMAND ----------

dfsc=spark.read.format('csv')\
  .option('header','true')\
  .option('inferSchema','true')\
  .option('delimiter',',')\
  .load(f'{rootFolder}/sales_data_corrupted.csv')
display(dfsc)

# COMMAND ----------

# DBTITLE 1,Dropping  all NA's
dsfcm1=dfsc.na.drop(how='all')
display(dsfcm1)

# COMMAND ----------

# DBTITLE 1,NA's in some columns
dsfcm1=dfsc.na.drop(subset=['product_name','client_name'])
display(dsfcm1)

# COMMAND ----------

# DBTITLE 1,Replacing missing values with specific values
dsfcm1=dfsc.na.fill({'product_name': 'Unknown','sales_amount':0})
display(dsfcm1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generating time dimension

# COMMAND ----------

# DBTITLE 1,Date dimension DDL with autogenerated attributes
sqlCmd=f""" CREATE OR REPLACE TABLE  {tablePath}.date_dim (
              date_id BIGINT GENERATED BY DEFAULT AS IDENTITY,
              date_value DATE,
              month_day INT GENERATED ALWAYS AS (dayofmonth(date_value)),
              month_num INT GENERATED ALWAYS AS (month(date_value)),
              month_name STRING GENERATED ALWAYS AS (date_format(date_value,'MMM')),
              year INT GENERATED ALWAYS AS (year(date_value))
               ) """ 
spark.sql(sqlCmd)
 


# COMMAND ----------

spark.sql(f'SELECT sales_date as date_value FROM {tablePath}.sales_bronze')\
    .write.mode('append')\
    .option("mergeSchema", "true")\
    .saveAsTable(f'{tablePath}.date_dim')

display(spark.table(f'{tablePath}.date_dim'))    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema normalization

# COMMAND ----------

# DBTITLE 1,Read fact/dimension tables
dfs=spark.table(f'{tablePath}.sales_bronze')
dfp=spark.table(f'{tablePath}.product_silver')

# COMMAND ----------

# DBTITLE 1,Normalizing fact tables
dfsp=dfs.join(dfp, on='product_name', how='left')\
    .select('sales_date','client_name','price','quantity','sales_amount','product_id')\
    .withColumn('product_id', when(col('product_id').isNull(), lit(-1)).otherwise(col('product_id')))
display(dfsp)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time travel

# COMMAND ----------

# DBTITLE 1,Browse history
tablePath='hive_metastore.default'
display(spark.sql(f"DESCRIBE HISTORY {tablePath}.product_silver"))

# COMMAND ----------

# DBTITLE 1,Querying earlier versions of table
display(spark.sql(f'SELECT * FROM {tablePath}.product_silver VERSION AS OF 3' ))

# COMMAND ----------

# DBTITLE 1,Restore table to earlier version
spark.sql(f"RESTORE TABLE {tablePath}.product_silver TO VERSION AS OF 3")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table clones (see https://docs.databricks.com/en/delta/clone.html for more details)

# COMMAND ----------

# DBTITLE 1,Deep cloning
spark.sql(f"CREATE OR REPLACE TABLE {tablePath}.product_silver_dclone CLONE {tablePath}.product_silver")
display(spark.table("product_silver_dclone"))

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE {tablePath}.product_silver_shclone SHALLOW CLONE {tablePath}.product_silver")
display(spark.table("product_silver_shclone"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table partitions

# COMMAND ----------

# DBTITLE 1,Partitioning
spark.table(f'{tablePath}.product_silver').write\
    .partitionBy('product_name')\
    .mode("append")\
    .saveAsTable(f'{tablePath}.product_silver_partitioned')

display(spark.sql(f'DESCRIBE TABLE EXTENDED {tablePath}.product_silver_partitioned'))

# COMMAND ----------

# DBTITLE 1,Inspecting folder structure for partitioned table
# MAGIC %fs
# MAGIC ls dbfs:/user/hive/warehouse/product_silver_partitioned

# COMMAND ----------


