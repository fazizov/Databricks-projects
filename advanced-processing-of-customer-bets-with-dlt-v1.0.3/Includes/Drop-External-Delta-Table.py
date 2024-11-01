# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

DA = DBAcademyHelper(course_config, lesson_config)

# COMMAND ----------

schema = "techsummit_2024_dlt_bets_lab"

truncateTableSql = """TRUNCATE TABLE {}.{}.bets_by_customer_event_agg""".format(DA.catalog_name, schema)
vacuumTableSql = """VACUUM {}.{}.bets_by_customer_event_agg RETAIN 0 HOURS""".format(DA.catalog_name, schema)
dropTableSql = """DROP TABLE IF EXISTS {}.{}.bets_by_customer_event_agg""".format(DA.catalog_name, schema)

spark.sql(truncateTableSql)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql(vacuumTableSql)
spark.sql(dropTableSql)
