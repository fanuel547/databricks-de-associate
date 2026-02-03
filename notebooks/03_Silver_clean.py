# Databricks notebook source
# MAGIC %md
# MAGIC Silver (Limpeza)

# COMMAND ----------

from pyspark.sql.functions import to_date
df_bronze = spark.table("projecto1.de_associate.bronze_sales_raw")

df_clean = (df_bronze.withColumn("order_date", to_date("order_date")).filter(df_bronze.quantity > 0))

df_clean.write.format("delta").mode("overwrite").saveAsTable("projecto1.de_associate.silver_sales_clean")