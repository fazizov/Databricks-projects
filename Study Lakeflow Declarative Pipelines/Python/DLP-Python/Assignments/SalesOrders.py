from pyspark import pipelines as dp
import pyspark.sql.functions as F
csv_schema='customer_id string, tax_id string, tax_code string, customer_name string, state string, city string, postcode string, street string, number string, unit string, region string, district string, lon string, lat string, ship_to_address string, valid_from string, valid_to string, units_purchased string, loyalty_segment string'

json_schema='clicked_items array<array<string>>, customer_id string, customer_name string, number_of_line_items string, order_datetime string, order_number bigint, ordered_products array<struct<curr:string,id:string,name:string,price:bigint,promotion_info:struct<promo_disc:double,promo_id:bigint,promo_item:string,promo_qty:bigint>,qty:bigint,unit:string>>, promo_info array<struct<promo_disc:double,promo_id:bigint,promo_item:string,promo_qty:bigint>>'

#Step 1. TO DO: add a streaming table for sales orders. The target table should be called 'sales'  and located in pipeline's default catalog's Bronze schema

@dp.table(name="sales")
def sales():
  return spark.readStream.format("json").schema(json_schema).load("dbfs:/databricks-datasets/retail-org/sales_orders/")

#Step 2. TO DO: Create a streaming table for customers. The target table should be called 'customers'  and located in pipeline's default catalog's Bronze schema.

@dp.table(name="customers")
def sales():
  return spark.readStream.format("csv").schema(csv_schema)\
      .load("dbfs:/databricks-datasets/retail-org/customers")


#Step 3. TO DO: Create a streaming table named 'sales_orders' in the Silver schema.The target table should include only the following columns: "order_number","order_datetime",customer_id, customer_name,number_of_line_items

@dp.table(name="silver.sales_orders")
def sales_orders():
  return spark.readStream.table("sales").select("order_number","order_datetime","customer_id","customer_name","number_of_line_items")

#Step 4. TO DO: Use DQ validation expectations to filter out  customers having empty city attributes.The target table should a streaming table named 'customers' and located in default catalog's Silver schema.The target table should include only the following columns: customer_id, customer_name, city, state,region

@dp.table(name="silver.customers")
@dp.expect_or_drop("city is not empty", "city != '' and city is not null")
@dp.expect("region is not empty", "region != '' and region is not null")
def sales():
  return spark.readStream.table("customers").select("customer_id","customer_name","city","state","region")


#Step 5. Create a materialized view named 'mv_sales_order_items' in the Silver schema, sourced from Bronze.sales table.The definition of materilazied view should include following transformations:
# Add Identity column named item_id, using monotonically_increasing_id() function.
# Parse ordered_products column to separate each ordered item, using explode function.
# Extract sub-fields under ordered_products
# Calculate amount field by multiplying qty to price column
# The table should include only the following columns: "item_id","order_number","product_id", "product_name", "currency","unit","price", "qty","amount"

@dp.materialized_view(name="silver.mv_sales_order_items")
def mv_sales_order_items():
  return spark.read.table("sales").withColumn("item", F.explode("ordered_products"))\
    .withColumn("item_id", F.monotonically_increasing_id())\
    .selectExpr("item_id","order_number","item.id as product_id", "item.name as product_name", "item.curr as currency", 
                 "item.unit","item.price", "item.qty")\
    .withColumn("amount",F.col("price")*F.col("qty"))\
    .select("item_id","order_number","product_id", "product_name", "currency","unit","price", "qty","amount")

#Step 6. TO DO: Create a materialized view named 'mv_sales_order_aggregates' in the Silver schema. Instructions:
# -Join silver.sales_orders and silver.mv_sales_order_items tables on order_number column.
# -Group by order_number, calculate the sum of amount column, and name it total_amount
# -The target table should be a Delta Lake table and should include only the following columns: "order_number","total_amount"

@dp.materialized_view(name="silver.mv_sales_order_aggregates")
def mv_sales_order_items():
  return spark.read.table("silver.sales_orders")\
    .join(spark.read.table("silver.mv_sales_order_items"), "order_number")\
    .groupBy("order_number")\
    .agg(F.sum("amount").alias("total_amount"))\

#Step 7.TO DO: Create a materialized view named 'mv_sales_orders_customers' in the Silver schema. Instructions:
# -Join silver.mv_sales_order_aggregates, silver.sales_orders and silver.customers tables on order_number column.
# -Group by order_number, calculate the sum of amount column, and name it total_amount
# -The target table should be a Delta Lake table and should include only the following columns: "order_number","order_datetime","number_of_line_items","total_amount",
# "C.customer_id","C.customer_name","city","state"

@dp.materialized_view(name="silver.mv_sales_orders_customers")
def mv_sales_orders_customers():
  return spark.read.table("silver.mv_sales_order_aggregates")\
    .join(spark.read.table("silver.sales_orders"), "order_number")\
    .join(spark.read.table("silver.customers").alias("C"),"customer_id")\
    .select("order_number","order_datetime","number_of_line_items","total_amount",
            "C.customer_id","C.customer_name","city","state")
  
#Step 8. TO DO: Create a materialized view named 'mv_sales_orders_aggregates_by_state' in the Silver schema. Instructions:
# -Group by "state","city" columns and calculate the sum of total_amount column, and name it as total_amount_$
# -The target table should be a Delta Lake table and should include only the following columns: "state","city","total_amount_$"
#-Order by "state","city" columns

@dp.materialized_view(name="silver.mv_sales_orders_aggregates_by_state")
def mv_sales_orders_aggregates_by_state():
  return spark.read.table("silver.mv_sales_orders_customers")\
    .groupBy("state","city")\
    .agg(F.sum("total_amount").alias("total_amount_$"))\
    .orderBy("state","city")
    

#Step 9. Create a Delta Lake sink named 'sales_orders_aggregates_delta_lake' in the Silver schema. The target table should be a Delta Lake table and should include only the following columns: "state","city","total_amount_$"
#Tip: Use catalog name paramater to specify the catalog name for the target table

uc_name= spark.conf.get('uc_name')


dp.create_sink(name='sales_orders_aggregates_delta_lake', format='delta',
                options={'tableName':f'{uc_name}.silver.sales_orders_aggregates_delta_lake'})

@dp.append_flow (name='sales_orders_aggregates_delta_lake_flow',target='sales_orders_aggregates_delta_lake')
def sales_orders_aggregates_delta_lake_flow():
  return spark.readStream.table('silver.mv_sales_orders_aggregates_by_state')






      
