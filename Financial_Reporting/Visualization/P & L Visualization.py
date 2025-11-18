# Databricks notebook source
# Revenue Trend
df = spark.table("gold_pnl").orderBy("year")
display(df.select("company", "year", "revenue"))


# COMMAND ----------

# Revenue vs COGS vs Gross Profit
df = spark.table("gold_pnl").orderBy("year")
display(df.select("company", "year", "revenue", "cogs", "gross_profit"))


# COMMAND ----------

# YoY Revenue Growth (%)
df = spark.table("gold_pnl").orderBy("year")
display(df.select("company", "year", "revenue_yoy_pct"))


# COMMAND ----------

# Profit Breakdown
df = spark.table("gold_pnl").orderBy("year")
display(df.select("company", "year", "gross_profit", "operating_income", "net_income"))
