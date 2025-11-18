# Databricks notebook source
# Operating / Investing / Financing Cash Flow
df = spark.table("gold_cashflow").orderBy("year")
display(df.select("company","year",
                  "cash_flow_operations","cash_flow_investing","cash_flow_financing"))


# COMMAND ----------

# Free Cash Flow
df = spark.table("gold_cashflow").orderBy("year")
display(df.select("company","year","free_cash_flow"))


# COMMAND ----------

# Net Change in Cash
df = spark.table("gold_cashflow").orderBy("year")
display(df.select("company","year","net_change_in_cash"))
