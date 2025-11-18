# Databricks notebook source
# Assets vs Liabilities vs Equity
df = spark.table("gold_balance_sheet").orderBy("year")
display(df.select("company","year","total_assets","total_liabilities","total_equity"))


# COMMAND ----------

# Working Capital Trend
df = spark.table("gold_balance_sheet").orderBy("year")
display(df.select("company","year","working_capital"))


# COMMAND ----------

# Δ Assets / Liabilities / Equity
df = spark.table("gold_balance_sheet").orderBy("year")
display(df.select("company","year","delta_assets","delta_liabilities","delta_equity"))


# COMMAND ----------

