from pyspark import pipelines as dp
import pyspark.sql.functions as F
csv_schema='customer_id string, tax_id string, tax_code string, customer_name string, state string, city string, postcode string, street string, number string, unit string, region string, district string, lon string, lat string, ship_to_address string, valid_from string, valid_to string, units_purchased string, loyalty_segment string'

json_schema='clicked_items array<array<string>>, customer_id string, customer_name string, number_of_line_items string, order_datetime string, order_number bigint, ordered_products array<struct<curr:string,id:string,name:string,price:bigint,promotion_info:struct<promo_disc:double,promo_id:bigint,promo_item:string,promo_qty:bigint>,qty:bigint,unit:string>>, promo_info array<struct<promo_disc:double,promo_id:bigint,promo_item:string,promo_qty:bigint>>'

#Step 1. TO DO: Add a streaming table for sales orders. The target table should be called 'sales'  and located in pipeline's default catalog's Bronze schema




#Step 2. TO DO: Create a streaming table for customers. The target table should be called 'customers'  and located in pipeline's default catalog's Bronze schema.





#Step 3. TO DO: Create a streaming table named 'sales_orders' in the Silver schema.The target table should include only the following columns: "order_number","order_datetime",customer_id, customer_name,number_of_line_items




#Step 4. TO DO: Use DQ validation expectations to filter out  customers having empty city attributes.The target table should a streaming table named 'customers' and located in default catalog's Silver schema.The target table should include only the following columns: customer_id, customer_name, city, state,region




#Step 5. Create a materialized view named 'mv_sales_order_items' in the Silver schema, sourced from Bronze.sales table.The definition of materilazied view should include following transformations:
# Add Identity column named item_id, using monotonically_increasing_id() function.
# Parse ordered_products column to separate each ordered item, using explode function.
# Extract sub-fields under ordered_products
# Calculate amount field by multiplying qty to price column
# The table should include only the following columns: "item_id","order_number","product_id", "product_name", "currency","unit","price", "qty","amount"


#Step 6. TO DO: Create a materialized view named 'mv_sales_order_aggregates' in the Silver schema. Instructions:
# -Join silver.sales_orders and silver.mv_sales_order_items tables on order_number column.
# -Group by order_number, calculate the sum of amount column, and name it total_amount
# -The target table should be a Delta Lake table and should include only the following columns: "order_number","total_amount"



#Step 7.TO DO: Create a materialized view named 'mv_sales_orders_customers' in the Silver schema. Instructions:
# -Join silver.mv_sales_order_aggregates, silver.sales_orders and silver.customers tables on order_number column.
# -Group by order_number, calculate the sum of amount column, and name it total_amount
# -The target table should be a Delta Lake table and should include only the following columns: "order_number","order_datetime","number_of_line_items","total_amount",
# "C.customer_id","C.customer_name","city","state"


  
#Step 8. TO DO: Create a materialized view named 'mv_sales_orders_aggregates_by_state' in the Silver schema. Instructions:
# -Group by "state","city" columns and calculate the sum of total_amount column, and name it as total_amount_$
# -The target table should be a Delta Lake table and should include only the following columns: "state","city","total_amount_$"
#-Order by "state","city" columns


    

#Step 9. Create a Delta Lake sink named 'sales_orders_aggregates_delta_lake' in the Silver schema. The target table should be a Delta Lake table and should include only the following columns: "state","city","total_amount_$"
#Tip: Use catalog name paramater to specify the catalog name for the target table






      
