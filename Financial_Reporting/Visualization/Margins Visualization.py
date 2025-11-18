# Databricks notebook source
# Margins Over Time
df = spark.table("gold_kpi").orderBy("year")
display(df.select("company", "year", "gross_margin", "operating_margin", "net_margin"))


# COMMAND ----------

# Rolling Margins
df = spark.table("gold_kpi").orderBy("year")
display(df.select("company", "year", "gross_margin_rolling3", "net_margin_rolling3"))


# COMMAND ----------

# KPI Table
df = spark.table("gold_kpi").orderBy("year")
display(df.select("company", "year", "roa", "roe"))


# COMMAND ----------

# ROA & ROE Over Time
df = spark.table("gold_kpi").orderBy("year")
display(df.select("company","year","roa","roe"))
