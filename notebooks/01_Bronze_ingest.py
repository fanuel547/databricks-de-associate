# Databricks notebook source
# DBTITLE 1,Untitled
path = "/Workspace/Users/fanuelquimbango2024@gmail.com/Projecto1/sales.csv"

df = (spark.read
      .option("header", "true")
      .option("inferschema", "true")
      .csv(path))

display(df)

# COMMAND ----------

# DBTITLE 1,Cell 2
df = spark.table("projecto1.de_associate.bronze_sales_raw")

display(df)
df.printSchema()
df.count()

# COMMAND ----------

from pyspark.sql.functions import col

df_bronze = df.withColumn(
    "line_total",
    col("quantity") * col("unit_price")
)

display(df_bronze)

# COMMAND ----------

df_bronze.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("projecto1.de_associate.delta_test_sales")

# COMMAND ----------

display(spark.table("projecto1.de_associate.delta_test_sales").where("category = 'Electronics'"))

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE projecto1.de_associate.delta_test_sales
# MAGIC SET unit_price = unit_price * 0.9
# MAGIC WHERE category = "Electronics";
# MAGIC
# MAGIC DELETE FROM projecto1.de_associate.delta_test_sales
# MAGIC WHERE quantity <= 0;

# COMMAND ----------

display(spark.table("projecto1.de_associate.delta_test_sales"))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY projecto1.de_associate.delta_test_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM projecto1.de_associate.delta_test_sales VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM projecto1.de_associate.delta_test_sales VERSION AS OF 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM projecto1.de_associate.delta_test_sales TIMESTAMP AS OF '2026-01-29'

# COMMAND ----------

updates = spark.createDataFrame([(1, 1300), (9, 50)], ["order_id", "unit_price"])

updates.createOrReplaceTempView("updates")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO projecto1.de_associate.delta_test_sales t
# MAGIC USING updates s
# MAGIC ON t.order_id = s.order_id
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET t.unit_price = s.unit_price
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT(order_id, unit_price)
# MAGIC   VALUES(s.order_id, s.unit_price)