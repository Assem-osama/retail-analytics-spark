from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    sum,
    count,
    countDistinct,
    abs,
    round,
    max,
    min
)


spark = (
    SparkSession.builder
    .appName("OnlineRetailII-GoldValidation")
    .master("spark://spark-master:7077")
    .config("spark.executor.cores", "1")
    .config("spark.executor.memory", "512m")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

SILVER_PATH = "/data/silver/online_retail"

DAILY_SALES_PATH = "/data/gold/daily_sales"
PRODUCT_PATH = "/data/gold/product_performance"
CUSTOMER_PATH = "/data/gold/customer"
COUNTRY_PATH = "/data/gold/country_sales"



print("GOLD DATA QUALITY VALIDATION STARTED")


# 1. READ DATA

silver_df = spark.read.parquet(SILVER_PATH)

daily_sales_df = spark.read.parquet(DAILY_SALES_PATH)

product_df = spark.read.parquet(PRODUCT_PATH)

customer_df = spark.read.parquet(CUSTOMER_PATH)

country_df = spark.read.parquet(COUNTRY_PATH)


print("\nSource / Gold Row Counts:")
print(f"Silver:          {silver_df.count():,}")
print(f"Daily Sales:     {daily_sales_df.count():,}")
print(f"Products:        {product_df.count():,}")
print(f"Customer Rows:   {customer_df.count():,}")
print(f"Countries:       {country_df.count():,}")


# 2. SOURCE BUSINESS METRICS

print("SOURCE BUSINESS METRICS")

source_metrics = silver_df.select(

    sum(
        when(
            (col("TransactionType") == "Normal") &
            (col("Quantity") > 0),
            col("Revenue")
        ).otherwise(0.0)
    ).alias("GrossSales"),

    sum(
        when(
            (col("TransactionType") == "Normal") &
            (col("Quantity") < 0),
            abs(col("Revenue"))
        ).otherwise(0.0)
    ).alias("ReturnAmount"),

    sum(
        when(
            col("TransactionType") == "Cancelled",
            abs(col("Revenue"))
        ).otherwise(0.0)
    ).alias("CancelledAmount"),

    sum(
        when(
            (col("TransactionType") == "Normal") &
            (col("Quantity") > 0),
            col("Quantity")
        ).otherwise(0)
    ).alias("UnitsSold"),

    sum(
        when(
            (col("TransactionType") == "Normal") &
            (col("Quantity") < 0),
            abs(col("Quantity"))
        ).otherwise(0)
    ).alias("ReturnUnits")

).first()


source_gross_sales = source_metrics["GrossSales"] or 0.0
source_return_amount = source_metrics["ReturnAmount"] or 0.0
source_cancelled_amount = source_metrics["CancelledAmount"] or 0.0
source_units_sold = source_metrics["UnitsSold"] or 0
source_return_units = source_metrics["ReturnUnits"] or 0

source_net_revenue = (source_gross_sales - source_return_amount - source_cancelled_amount)


print(f"Gross Sales:       {source_gross_sales:,.2f}")
print(f"Return Amount:     {source_return_amount:,.2f}")
print(f"Cancelled Amount:  {source_cancelled_amount:,.2f}")
print(f"Units Sold:        {source_units_sold:,}")
print(f"Return Units:      {source_return_units:,}")
print(f"Net Revenue:       {source_net_revenue:,.2f}")


# 3. GOLD RECONCILIATION

print("GOLD RECONCILIATION")

gold_metrics = product_df.select(

    sum("GrossSales").alias("GrossSales"),

    sum("ReturnAmount").alias("ReturnAmount"),

    sum("CancelledAmount").alias("CancelledAmount"),

    sum("UnitsSold").alias("UnitsSold"),

    sum("ReturnUnits").alias("ReturnUnits")

).first()


gold_gross_sales = gold_metrics["GrossSales"] or 0.0
gold_return_amount = gold_metrics["ReturnAmount"] or 0.0
gold_cancelled_amount = gold_metrics["CancelledAmount"] or 0.0
gold_units_sold = gold_metrics["UnitsSold"] or 0
gold_return_units = gold_metrics["ReturnUnits"] or 0

gold_net_revenue = (gold_gross_sales - gold_return_amount - gold_cancelled_amount)


print(f"Gold Gross Sales:       {gold_gross_sales:,.2f}")
print(f"Gold Return Amount:     {gold_return_amount:,.2f}")
print(f"Gold Cancelled Amount:  {gold_cancelled_amount:,.2f}")
print(f"Gold Units Sold:        {gold_units_sold:,}")
print(f"Gold Return Units:      {gold_return_units:,}")
print(f"Gold Net Revenue:       {gold_net_revenue:,.2f}")


# 4. RECONCILIATION DIFFERENCES

print("\nReconciliation Differences:")

gross_difference = (
    source_gross_sales - gold_gross_sales
)

return_difference = (
    source_return_amount - gold_return_amount
)

cancelled_difference = (
    source_cancelled_amount - gold_cancelled_amount
)

units_difference = (
    source_units_sold - gold_units_sold
)

return_units_difference = (
    source_return_units - gold_return_units
)

net_difference = (
    source_net_revenue - gold_net_revenue
)


print(f"Gross Sales Difference:      {gross_difference:,.2f}")
print(f"Return Amount Difference:    {return_difference:,.2f}")
print(f"Cancelled Difference:        {cancelled_difference:,.2f}")
print(f"Units Sold Difference:       {units_difference:,}")
print(f"Return Units Difference:     {return_units_difference:,}")
print(f"Net Revenue Difference:      {net_difference:,.2f}")


# 5. RETURN / CANCEL OVERLAP CHECK

print("RETURN / CANCEL OVERLAP CHECK")

overlap_count = silver_df.filter(
    (col("TransactionType") == "Cancelled") &
    (col("Quantity") < 0)
).count()


print(
    f"Cancelled transactions with negative quantity: "
    f"{overlap_count:,}"
)

# 6. PRODUCT MART DUPLICATES

print("PRODUCT MART DUPLICATE CHECK")

product_duplicate_count = (
    product_df
    .groupBy("StockCode")
    .count()
    .filter(col("count") > 1)
    .count()
)


print(
    f"Product StockCodes with multiple rows: "
    f"{product_duplicate_count:,}"
)


# 7. COUNTRY MART DUPLICATES

print("COUNTRY MART DUPLICATE CHECK")

country_duplicate_count = (
    country_df
    .groupBy("Country")
    .count()
    .filter(col("count") > 1)
    .count()
)

print(
    f"Countries with multiple rows: "
    f"{country_duplicate_count:,}"
)

# 8. CUSTOMER MART DUPLICATES

print("CUSTOMER MART GRAIN CHECK")

customer_duplicate_count = (
    customer_df
    .groupBy("Customer ID", "Country")
    .count()
    .filter(col("count") > 1)
    .count()
)


print(
    f"Duplicate Customer ID + Country combinations: "
    f"{customer_duplicate_count:,}"
)


# 9. NULL CHECKS

print("GOLD NULL CHECK")

print("Product StockCode NULL:",
    product_df.filter(col("StockCode").isNull()).count())

print("Country NULL:",
    country_df.filter(col("Country").isNull()).count())

print("Customer ID NULL:",
    customer_df.filter(col("Customer ID").isNull()).count())


# 10. NEGATIVE NET REVENUE

print("NEGATIVE NET REVENUE CHECK")

negative_product_net = product_df.filter(
    col("NetRevenue") < 0
).count()


negative_customer_net = customer_df.filter(
    col("NetRevenue") < 0
).count()


negative_country_net = country_df.filter(
    col("NetRevenue") < 0
).count()


print(
    f"Products with negative NetRevenue:  "
    f"{negative_product_net:,}"
)

print(
    f"Customers with negative NetRevenue: "
    f"{negative_customer_net:,}"
)

print(
    f"Countries with negative NetRevenue: "
    f"{negative_country_net:,}"
)


# 11. RANK VALIDATION

print("RANK VALIDATION")

print(
    f"Max Product Rank:  "
    f"{product_df.agg(max('RevenueRank')).first()[0]}"
)

print(
    f"Max Customer Rank: "
    f"{customer_df.agg(max('CustomerRank')).first()[0]}"
)

print(
    f"Max Country Rank:  "
    f"{country_df.agg(max('CountryRank')).first()[0]}"
)


# 12. FINAL 

import time
print("Waiting for 5 minutes so you can check the Spark UI at http://localhost:4040")
time.sleep(300)

spark.stop()