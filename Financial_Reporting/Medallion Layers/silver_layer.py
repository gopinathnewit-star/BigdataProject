# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
# from pyspark.sql import functions as F

# COMMAND ----------

df_silver = spark.read.table("workspace.default.finance_bronze")
display(df_silver)

# COMMAND ----------

# CLEAN COLUMN NAMES

df_silver = df_silver.toDF(*[
    c.lower()
     .strip()
     .replace(" ", "_")
     .replace("(", "")
     .replace(")", "")
     .replace("-", "_")
    for c in df_silver.columns
])


# COMMAND ----------

# TRIM TEXT FIELDS & STANDARDIZE DATE

if "period" in df_silver.columns:
    df_silver = df_silver.withColumn("period", trim(col("period")))
    df_silver = df_silver.withColumn("period", to_date(col("period"), "yyyy-MM-dd"))

# COMMAND ----------

# REMOVE DUPLICATES

df_silver = df_silver.dropDuplicates()

# COMMAND ----------

# --------- CLEAN NUMERIC COLUMNS FIRST (VERY IMPORTANT) ---------

for colname in financial_cols:
    if colname in df_silver.columns:

        df_silver = (
            df_silver

            # Remove commas and spaces
            .withColumn(colname, F.regexp_replace(colname, ",", ""))

            # Extract the FIRST valid number (optional sign + digits + optional decimal)
            .withColumn(
                colname,
                F.regexp_extract(
                    colname,
                    r"([-+]?[0-9]*\.?[0-9]+)",
                    1
                )
            )

            # Convert empty string to NULL
            .withColumn(
                colname,
                F.when(F.col(colname) == "", None).otherwise(F.col(colname))
            )

            # Final SAFE cast
            .withColumn(colname, F.col(colname).cast("double"))
        )


# COMMAND ----------

# REMOVE ROWS WITH ALL NULL FINANCIAL VALUES

financial_cols = [
    "revenue", "cogs", "operating_expense", "interest_expense",
    "tax_expense", "net_income", "total_assets", "total_liabilities",
    "total_equity", "cash_flow_operations", "cash_flow_investing",
    "cash_flow_financing", "net_change_in_cash"
]

df_silver = df_silver.dropna(subset=financial_cols, how="all")

# COMMAND ----------

# FIX NUMERIC COLUMNS (remove commas, cast to double)

for colname in financial_cols:
    if colname in df_silver.columns:
        df_silver = df_silver.withColumn(colname, regexp_replace(col(colname), ",", ""))
        df_silver = df_silver.withColumn(colname, col(colname).cast("double"))

# COMMAND ----------

# REMOVE NEGATIVE VALUES WHERE NOT VALID

non_negative_cols = [
    "revenue", "cogs", "operating_expense", "interest_expense",
    "tax_expense", "net_income", "total_assets",
    "total_liabilities", "total_equity"
]

for colname in non_negative_cols:
    if colname in df_silver.columns:
        df_silver = df_silver.filter((col(colname).isNull()) | (col(colname) >= 0))

# COMMAND ----------

#  FIX BALANCE SHEET CONSISTENCY
# total_equity = total_assets - total_liabilities

if set(["total_assets", "total_liabilities", "total_equity"]).issubset(df_silver.columns):
    df_silver = df_silver.withColumn(
        "total_equity",
        col("total_assets") - col("total_liabilities")
    )

# COMMAND ----------

# 8️⃣ HANDLE OUTLIERS (3 standard deviation rule)

for colname in financial_cols:
    if colname in df_silver.columns:

        # compute mean and std
        stats = df_silver.select(
            F.mean(colname).alias("mean"),
            F.stddev(colname).alias("std")
        ).collect()[0]

        mean_val = stats["mean"]
        std_val  = stats["std"]

        # Skip if column too dirty or small
        if mean_val is None or std_val is None or std_val == 0:
            print(f"Skipping outlier detection for {colname} — insufficient clean data")
            continue

        # Apply 3σ rule
        df_silver = df_silver.filter(
            (F.col(colname) >= mean_val - 3 * std_val) &
            (F.col(colname) <= mean_val + 3 * std_val)
        )


# COMMAND ----------

# 9️⃣ ADD DERIVED METRICS

df_silver = df_silver.withColumn("gross_profit", col("revenue") - col("cogs"))
df_silver = df_silver.withColumn("operating_income", col("gross_profit") - col("operating_expense"))
df_silver = df_silver.withColumn("earning_before_tax", col("operating_income") - col("interest_expense"))
df_silver = df_silver.withColumn("net_income", col("earning_before_tax") - col("tax_expense"))

df_silver = df_silver.withColumn("gross_margin", col("gross_profit") / col("revenue"))
df_silver = df_silver.withColumn("operating_margin", col("operating_income") / col("revenue"))
df_silver = df_silver.withColumn("net_margin", col("net_income") / col("revenue"))

df_silver = df_silver.withColumn("roe", col("net_income") / col("total_equity"))
df_silver = df_silver.withColumn("roa", col("net_income") / col("total_assets"))

# COMMAND ----------

# 🔟 FINAL SILVER TABLE OVERWRITE

df_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("finance_silver")

display(df_silver)