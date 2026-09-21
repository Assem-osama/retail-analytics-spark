from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    when,
    regexp_replace,
    count,
    sum
)


spark = (
    SparkSession.builder
    .appName("OnlineRetailII-Silver")
    .master("spark://spark-master:7077")
    .config("spark.executor.cores", "1")
    .config("spark.executor.memory", "512m")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


BRONZE_DATA_PATH = "/data/bronze/online_retail"
SILVER_DATA_PATH = "/data/silver/online_retail"


print("SILVER TRANSFORMATION STARTED")

# 1. READ BRONZE DATA

bronze_df = spark.read.parquet(BRONZE_DATA_PATH)

print("\nBronze Data:")
print(f"Rows: {bronze_df.count():,}")
print(f"Columns: {len(bronze_df.columns)}")

print("\nBronze Schema:")
bronze_df.printSchema()

print("\nBronze Partitions:")
print(bronze_df.rdd.getNumPartitions())


# 2. REMOVE EXACT DUPLICATES

before_dedup = bronze_df.count()

silver_df = bronze_df.dropDuplicates()

after_dedup = silver_df.count()

removed_duplicates = before_dedup - after_dedup

print("\nDuplicate Removal:")
print(f"Before:  {before_dedup:,}")
print(f"After:   {after_dedup:,}")
print(f"Removed: {removed_duplicates:,}")


# 3. CLEAN STRING COLUMNS

silver_df = (
    silver_df
    .withColumn("Invoice", trim(col("Invoice")))
    .withColumn("StockCode", trim(col("StockCode")))
    .withColumn("Description", trim(col("Description")))
    .withColumn("Country", trim(col("Country")))
)


# 4. CLEAN CUSTOMER ID

silver_df = silver_df.withColumn(
    "Customer ID",
    when(
        col("Customer ID").isNotNull(),
        regexp_replace(
            col("Customer ID"),
            r"\.0$",
            ""
        )
    ).otherwise(None)
)


# 5. CREATE TRANSACTION TYPE

silver_df = silver_df.withColumn(
    "TransactionType",
    when(
        col("Invoice").startswith("C"),
        "Cancelled"
    ).otherwise("Normal")
)


# 6. IDENTIFY RETURNS

silver_df = silver_df.withColumn(
    "IsReturn",
    when(
        col("Quantity") < 0,
        True
    ).otherwise(False)
)


# 7. CALCULATE REVENUE

silver_df = silver_df.withColumn(
    "Revenue",
    col("Quantity") * col("Price")
)


# 8. SILVER TRANSFORMATION VALIDATION

print("\n" + "=" * 80)
print("SILVER TRANSFORMATION VALIDATION")
print("=" * 80)

print("\nSilver Schema:")
silver_df.printSchema()

print("\nSilver Sample:")
silver_df.show(10, truncate=False)


# 9. TRANSACTION TYPE ANALYSIS

print("\nTransaction Types:")

silver_df.groupBy(
    "TransactionType"
).count().show()


# 10. RETURN ANALYSIS

print("\nReturn Statistics:")

silver_df.groupBy(
    "IsReturn"
).count().show()


# 11. REVENUE STATISTICS

print("\nRevenue Statistics:")

silver_df.select(
    "Quantity",
    "Price",
    "Revenue"
).describe().show()


# 12. DATA QUALITY REPORT

print("DATA QUALITY REPORT")

quality_summary = silver_df.select(

    count("*").alias("TotalRows"),

    sum(
        when(col("Price") <= 0,
            1
        ).otherwise(0)
    ).alias("InvalidPriceRows"),

    sum(
        when(col("Quantity") == 0,
            1
        ).otherwise(0)
    ).alias("ZeroQuantityRows"),

    sum(
        when(col("Customer ID").isNull(),
            1
        ).otherwise(0)
    ).alias("MissingCustomerRows"),

    sum(
        when(col("Description").isNull(),
            1
        ).otherwise(0)
    ).alias("MissingDescriptionRows"),

    sum(
        when(col("InvoiceDate").isNull(),
            1
        ).otherwise(0)
    ).alias("MissingDateRows")
)

quality_summary.show(truncate=False)


# 13. APPLY SILVER DATA QUALITY RULES

print("\nApplying Silver Data Quality Rules...")

valid_silver_df = silver_df.filter(
    col("Price") > 0
)

before_quality = silver_df.count()
after_quality = valid_silver_df.count()

removed_invalid_records = before_quality - after_quality

print("\nSilver Quality Results:")
print(f"Before Quality Filter: {before_quality:,}")
print(f"After Quality Filter:  {after_quality:,}")
print(f"Removed Invalid Price: {removed_invalid_records:,}")


# 14. VALIDATE INVALID PRICES WERE REMOVED

remaining_invalid_prices = valid_silver_df.filter(
    col("Price") <= 0
).count()

print(
    f"Remaining Invalid Prices: "
    f"{remaining_invalid_prices:,}"
)


# 15. FINAL TRANSACTION TYPE VALIDATION

print("\nFinal Transaction Types:")

valid_silver_df.groupBy(
    "TransactionType"
).count().show()


# 16. FINAL RETURN VALIDATION

print("\nFinal Return Statistics:")

valid_silver_df.groupBy(
    "IsReturn"
).count().show()


# 17. WRITE SILVER DATA

print("\nWriting Silver data...")

(
    valid_silver_df
    .write
    .mode("overwrite")
    .parquet(SILVER_DATA_PATH)
)


# 18. READ SILVER DATA FOR FINAL VALIDATION

silver_final_df = spark.read.parquet(
    SILVER_DATA_PATH
)


# 19. FINAL SILVER VALIDATION

print("\n" + "=" * 80)
print("FINAL SILVER VALIDATION")
print("=" * 80)

print(
    f"Final Silver Rows: "
    f"{silver_final_df.count():,}"
)

print(
    f"Final Silver Columns: "
    f"{len(silver_final_df.columns)}"
)

print("\nFinal Silver Schema:")

silver_final_df.printSchema()

print("\nFinal Silver Sample:")

silver_final_df.show(
    10,
    truncate=False
)


# 20. COMPLETED

print("\n" + "=" * 80)
print("SILVER TRANSFORMATION COMPLETED")
print("=" * 80)


import time
print("Waiting for 5 minutes so you can check the Spark UI... ⏳")
time.sleep(300)
spark.stop()

spark.stop()