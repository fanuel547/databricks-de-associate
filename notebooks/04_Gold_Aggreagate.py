# Databricks notebook source
# MAGIC %md
# MAGIC Gold (Analytics)
# MAGIC

# COMMAND ----------


from pyspark.sql.functions import sum as _sum

df_silver = spark.table("projecto1.de_associate.silver_sales_clean")

gold = df_silver.groupBy("country","category").agg(_sum("line_total").alias("total_sales"))

gold.write.format("delta").mode("overwrite").saveAsTable("projecto1.de_associate.gold_sales_summary")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW projecto1.de_associate.v_sales_angola
# MAGIC AS 
# MAGIC SELECT * FROM projecto1.de_associate.gold_sales_summary
# MAGIC WHERE country = 'Angola'