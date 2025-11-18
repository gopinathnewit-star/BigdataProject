# Databricks notebook source
# Completeness (%)
df = spark.table("gold_completeness").orderBy("year")
display(df.select("company","year","completeness_pct"))


# COMMAND ----------

# Missing Years
df = spark.table("gold_missing_years")
display(df)


# COMMAND ----------

