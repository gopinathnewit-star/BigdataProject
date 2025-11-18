# Databricks notebook source
df_silver = spark.table("workspace.default.finance_silver")
df_silver.printSchema()
display(df_silver.limit(5))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# 0. Config / load silver table
silver_table = "workspace.default.finance_silver"   # change if different
df = spark.table(silver_table)

# COMMAND ----------

# 1. Robustly clean string numeric columns that are currently strings
string_num_cols = ["ebit", "cash", "inventory", "receivables", "payables"]
for c in string_num_cols:
    if c in df.columns:
        # Extract first valid numeric token and cast to double
        df = df.withColumn(c,
            F.regexp_replace(F.col(c).cast("string"), ",", "")    # remove commas
        ).withColumn(c,
            F.regexp_extract(F.col(c), r"([-+]?[0-9]*\.?[0-9]+)", 1)
        ).withColumn(c,
            F.when(F.col(c) == "", None).otherwise(F.col(c).cast("double"))
        )

# COMMAND ----------

# 2. Ensure year is integer / long (it is long in your schema) and company exists
df = df.withColumn("year", F.col("year").cast("int"))
if "company" not in df.columns:
    # if company missing, create a default single company to allow partitioning
    df = df.withColumn("company", F.lit("Company"))

# COMMAND ----------

# 3. Derive missing financial fields safely
# gross_profit, operating_income, net_income, earning_before_tax
if "gross_profit" not in df.columns and {"revenue", "cogs"}.issubset(df.columns):
    df = df.withColumn("gross_profit", F.col("revenue") - F.col("cogs"))

if "operating_income" not in df.columns and {"gross_profit", "operating_expense"}.issubset(df.columns):
    df = df.withColumn("operating_income", F.col("gross_profit") - F.col("operating_expense"))

# Prefer existing net_income; else try deriving from operating_income -> interest -> tax
if "net_income" not in df.columns:
    if set(["operating_income","interest_expense","tax_expense"]).issubset(df.columns):
        df = df.withColumn("net_income", F.col("operating_income") - F.col("interest_expense") - F.col("tax_expense"))
    elif "operating_income" in df.columns:
        df = df.withColumn("net_income", F.col("operating_income"))
    else:
        df = df.withColumn("net_income", F.lit(None))

# Ensure totals consistency: if total_equity missing and assets/liabilities present, derive it
if "total_equity" not in df.columns and set(["total_assets","total_liabilities"]).issubset(df.columns):
    df = df.withColumn("total_equity", F.col("total_assets") - F.col("total_liabilities"))

# COMMAND ----------

# 4. Create window specs partitioned by company and ordered by year
w_company = Window.partitionBy("company").orderBy("year")
w_company_rolling3 = Window.partitionBy("company").orderBy("year").rowsBetween(-2, 0)  # 3-year rolling

# helper: show sample cleaned rows
print("Sample cleaned silver (schema and head):")
df.printSchema()
display(df.select("company","year","revenue","net_income","gross_profit","operating_income").limit(10))


# COMMAND ----------

# GOLD 1: P&L (yearly) -> gold_pnl
pnl_cols = ["company","year","revenue","cogs","gross_profit","operating_income","operating_expense",
            "interest_expense","tax_expense","net_income"]
pnl_cols = [c for c in pnl_cols if c in df.columns]

df_pnl = df.select(*pnl_cols)

# YoY growth (annual data -> lag 1)
df_pnl = df_pnl.withColumn("revenue_prev_year", F.lag("revenue", 1).over(w_company)) \
               .withColumn("revenue_yoy_pct", 
                           F.when(F.col("revenue_prev_year").isNotNull(),
                                  (F.col("revenue") - F.col("revenue_prev_year"))/F.col("revenue_prev_year")
                           ).otherwise(None)) \
               .withColumn("gross_margin", F.when(F.col("revenue") != 0, F.col("gross_profit")/F.col("revenue"))) \
               .withColumn("operating_margin", F.when(F.col("revenue") != 0, F.col("operating_income")/F.col("revenue"))) \
               .withColumn("net_margin", F.when(F.col("revenue") != 0, F.col("net_income")/F.col("revenue")))

# Persist
df_pnl.write.format("delta").mode("overwrite").saveAsTable("gold_pnl")
print("gold_pnl written")
display(df_pnl.orderBy("company","year").limit(20))

# COMMAND ----------

# GOLD 2: Balance Sheet Snapshot -> gold_balance_sheet
bs_cols = ["company","year","total_assets","total_liabilities","total_equity","inventory","receivables","payables","cash"]
bs_cols = [c for c in bs_cols if c in df.columns]
df_bs = df.select(*bs_cols)

# Derived metrics
if set(["total_assets","total_liabilities"]).issubset(df_bs.columns):
    df_bs = df_bs.withColumn("working_capital", F.col("total_assets") - F.col("total_liabilities"))

if set(["inventory","receivables","payables"]).issubset(df_bs.columns):
    df_bs = df_bs.withColumn("current_operating_assets", F.col("inventory") + F.col("receivables") - F.col("payables"))

# Roll-forwards: changes vs prior year
if "total_assets" in df_bs.columns:
    df_bs = df_bs.withColumn("delta_assets", F.col("total_assets") - F.lag("total_assets", 1).over(w_company))
if "total_liabilities" in df_bs.columns:
    df_bs = df_bs.withColumn("delta_liabilities", F.col("total_liabilities") - F.lag("total_liabilities", 1).over(w_company))
if "total_equity" in df_bs.columns:
    df_bs = df_bs.withColumn("delta_equity", F.col("total_equity") - F.lag("total_equity", 1).over(w_company))

# Persist
df_bs.write.format("delta").mode("overwrite").saveAsTable("gold_balance_sheet")
print("gold_balance_sheet written")
display(df_bs.orderBy("company","year").limit(20))


# COMMAND ----------

# GOLD 3: Cash Flow (using available fields) -> gold_cashflow
# prefer cash_flow_operations if available, else estimate
cf_cols_available = [c for c in ["company","year","cash_flow_operations","cash_flow_investing","cash_flow_financing","net_change_in_cash","cash"] if c in df.columns]
cf = df.select(*cf_cols_available)

if "cash_flow_operations" not in cf.columns:
    # derive approx operating CF = net_income - delta_assets + delta_liabilities (if available)
    tmp = df.select("company","year","net_income","total_assets","total_liabilities")
    tmp = tmp.withColumn("delta_assets", F.col("total_assets") - F.lag("total_assets", 1).over(w_company)) \
             .withColumn("delta_liabilities", F.col("total_liabilities") - F.lag("total_liabilities", 1).over(w_company)) \
             .withColumn("cash_flow_operations_est", 
                         F.col("net_income") - F.col("delta_assets") + F.col("delta_liabilities"))
    cf = tmp.select("company","year","net_income","cash_flow_operations_est") \
           .withColumnRenamed("cash_flow_operations_est","cash_flow_operations")

# Free cash flow: if capex exists use it, else try cash_flow_investing as proxy
if "capex" in df.columns:
    cf = cf.join(df.select("company","year","capex"), on=["company","year"], how="left") \
           .withColumn("free_cash_flow", F.col("cash_flow_operations") - F.col("capex"))
elif "cash_flow_investing" in cf.columns:
    cf = cf.withColumn("free_cash_flow", F.col("cash_flow_operations") + F.col("cash_flow_investing"))
else:
    cf = cf.withColumn("free_cash_flow", F.lit(None))

cf.write.format("delta").mode("overwrite").saveAsTable("gold_cashflow")
print("gold_cashflow written")
display(cf.orderBy("company","year").limit(20))

# COMMAND ----------

# GOLD 4: KPI Table -> gold_kpi
kpi_cols = ["company","year","revenue","gross_profit","operating_income","net_income","total_assets","total_equity"]
kpi_cols = [c for c in kpi_cols if c in df.columns]
df_kpi = df.select(*kpi_cols)

df_kpi = df_kpi.withColumn("gross_margin", F.when(F.col("revenue") != 0, F.col("gross_profit")/F.col("revenue"))) \
               .withColumn("operating_margin", F.when(F.col("revenue") != 0, F.col("operating_income")/F.col("revenue"))) \
               .withColumn("net_margin", F.when(F.col("revenue") != 0, F.col("net_income")/F.col("revenue"))) \
               .withColumn("roe", F.when(F.col("total_equity") != 0, F.col("net_income")/F.col("total_equity"))) \
               .withColumn("roa", F.when(F.col("total_assets") != 0, F.col("net_income")/F.col("total_assets")))

# rolling 3-year averages
df_kpi = df_kpi.withColumn("gross_margin_rolling3", F.avg("gross_margin").over(w_company_rolling3)) \
               .withColumn("net_margin_rolling3", F.avg("net_margin").over(w_company_rolling3))

df_kpi.write.format("delta").mode("overwrite").saveAsTable("gold_kpi")
print("gold_kpi written")
display(df_kpi.orderBy("company","year").limit(20))

# COMMAND ----------

# GOLD 5: Completeness & Missing Years Detection -> gold_completeness, gold_missing_years
# required fields per company-year
required = [c for c in ["revenue","cogs","operating_expense","net_income","total_assets","total_liabilities"] if c in df.columns]

# completeness: for each company-year what percent of required non-null fields are present
if len(required) > 0:
    completeness = df.select("company","year", *[
        F.when(F.col(c).isNotNull(), 1).otherwise(0).alias(c + "_present") for c in required
    ]).groupBy("company","year").agg(*[F.sum(c + "_present").alias(c + "_present_sum") for c in required])

    # join with rows count
    rows = df.groupBy("company","year").agg(F.count(F.lit(1)).alias("rows_loaded"))
    comp_summary = completeness.join(rows, on=["company","year"], how="left")

    total_required = len(required)
    # completeness_pct: sum(present) / (rows_loaded * total_required) * 100 OR simpler: sum_present / total_required *100 (per company-year)
    # using per-company-year sum_present / total_required gives fraction of required fields available (not influenced by rows_loaded)
    comp_summary = comp_summary.withColumn("completeness_pct",
                                           (sum([F.col(c + "_present_sum") for c in required]) / (F.lit(total_required))) * 100)

    comp_summary.write.format("delta").mode("overwrite").saveAsTable("gold_completeness")
    print("gold_completeness written")
    display(comp_summary.orderBy("company","year").limit(30))
else:
    print("No required fields present for completeness calculation.")

# Missing years per company: build expected (company, year) grid and find missing
min_year = df.agg(F.min("year")).collect()[0][0]
max_year = df.agg(F.max("year")).collect()[0][0]

if min_year is not None and max_year is not None:
    # create DataFrame of expected years
    years_df = spark.createDataFrame([(y,) for y in range(min_year, max_year+1)], ["year"])
    # distinct companies
    companies_df = df.select("company").distinct()
    # cross-join to expected (company,year)
    expected = companies_df.crossJoin(years_df)
    actual = df.select("company","year").distinct()
    missing = expected.join(actual, on=["company","year"], how="left_anti")  # expected not in actual
    missing.write.format("delta").mode("overwrite").saveAsTable("gold_missing_years")
    print("gold_missing_years written")
    display(missing.orderBy("company","year").limit(100))
else:
    print("Unable to compute missing years - min/max year not available.")

# COMMAND ----------

# ----------------------------------------
# Show locations for created tables (optional)
# ----------------------------------------
for t in ["gold_pnl","gold_balance_sheet","gold_cashflow","gold_kpi","gold_completeness","gold_missing_years"]:
    try:
        print(t)
        display(spark.sql(f"DESCRIBE DETAIL {t}").select("location"))
    except Exception as e:
        print("Could not describe table", t, ":", e)

print("Gold layer generation completed.")

# COMMAND ----------


