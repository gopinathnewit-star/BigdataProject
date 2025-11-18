# Databricks notebook source
df_bronze = spark.read.table("workspace.default.final_dataset")
display(df_bronze)

# COMMAND ----------

df_bronze.write.format("delta").mode("overwrite").saveAsTable("finance_bronze")

# COMMAND ----------

