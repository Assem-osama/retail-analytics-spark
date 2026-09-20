from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType
)

spark = (
    SparkSession.builder
    .appName("OnlineRetailII-Exploration")
    .master("spark://spark-master:7077")
    .config("spark.executor.cores", "1")
    .config("spark.executor.memory", "512m")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

RAW_DATA_PATH = "/data/raw/online_retail_II.csv"

retail_schema = StructType([
    StructField("Invoice", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", TimestampType(), True),
    StructField("Price", DoubleType(), True),
    StructField("Customer ID", StringType(), True),
    StructField("Country", StringType(), True)
])


# Read Raw Data

retail_df = (
    spark.read
    .option("header", True)
    .schema(retail_schema)
    .csv(RAW_DATA_PATH)
)


# 1. Dataset Overview
print("ONLINE RETAIL II - DATA EXPLORATION")

print(f"Spark Version: {spark.version}")
print(f"Number of Columns: {len(retail_df.columns)}")


# 2. Schema
print("SCHEMA")

retail_df.printSchema()


# 3. Row Count
print("\n" + "=" * 80)
print("ROW COUNT")
print("=" * 80)

total_rows = retail_df.count()

print(f"Total Rows: {total_rows:,}")


# 4. Sample Data

print("\n" + "=" * 80)
print("SAMPLE DATA")
print("=" * 80)

retail_df.show(10, truncate=False)


# 5. Null Analysis

print("\n" + "=" * 80)
print("NULL ANALYSIS")
print("=" * 80)

null_analysis = retail_df.select([
    F.sum(
        F.col(column).isNull().cast("int")
    ).alias(column)
    for column in retail_df.columns
])

null_analysis.show()


# 6. Distinct Values

print("\n" + "=" * 80)
print("DISTINCT COUNTS")
print("=" * 80)

distinct_counts = retail_df.select(
    F.countDistinct("Invoice").alias("Distinct Invoices"),
    F.countDistinct("StockCode").alias("Distinct Products"),
    F.countDistinct("Description").alias("Distinct Descriptions"),
    F.countDistinct("Customer ID").alias("Distinct Customers"),
    F.countDistinct("Country").alias("Distinct Countries")
)

distinct_counts.show()


# 7. Date Range

print("\n" + "=" * 80)
print("DATE RANGE")
print("=" * 80)

date_range = retail_df.select(
    F.min("InvoiceDate").alias("Minimum Invoice Date"),
    F.max("InvoiceDate").alias("Maximum Invoice Date")
)

date_range.show(truncate=False)


# 8. Quantity Statistics

print("\n" + "=" * 80)
print("QUANTITY STATISTICS")
print("=" * 80)

retail_df.select("Quantity").describe().show()


# 9. Price Statistics

print("\n" + "=" * 80)
print("PRICE STATISTICS")
print("=" * 80)

retail_df.select("Price").describe().show()


# 10. Negative Quantities

print("\n" + "=" * 80)
print("NEGATIVE QUANTITIES")
print("=" * 80)

negative_quantity_count = retail_df.filter(
    F.col("Quantity") < 0
).count()

print(
    f"Rows with negative quantity: "
    f"{negative_quantity_count:,}"
)


# 11. Invalid Prices

print("\n" + "=" * 80)
print("INVALID PRICES")
print("=" * 80)

zero_price_count = retail_df.filter(
    F.col("Price") == 0
).count()

negative_price_count = retail_df.filter(
    F.col("Price") < 0
).count()

invalid_price_count = retail_df.filter(
    F.col("Price") <= 0
).count()

print(f"Zero price rows:     {zero_price_count:,}")
print(f"Negative price rows: {negative_price_count:,}")
print(f"Invalid price rows:  {invalid_price_count:,}")


# 12. Cancelled Invoices

print("\n" + "=" * 80)
print("CANCELLED INVOICES")
print("=" * 80)

cancelled_invoice_count = retail_df.filter(
    F.col("Invoice").startswith("C")
).count()

print(
    f"Rows with cancelled invoices: "
    f"{cancelled_invoice_count:,}"
)


# 13. Duplicate Rows

print("\n" + "=" * 80)
print("DUPLICATE ROWS")
print("=" * 80)

distinct_row_count = retail_df.dropDuplicates().count()

duplicate_count = total_rows - distinct_row_count

print(f"Duplicate rows: {duplicate_count:,}")


# 14. Countries

print("\n" + "=" * 80)
print("COUNTRIES")
print("=" * 80)

retail_df.groupBy("Country").count().orderBy(
    F.desc("count")
).show(50, truncate=False)


# 15. Cancelled vs Normal Invoices

print("\n" + "=" * 80)
print("INVOICE TYPES")
print("=" * 80)

invoice_types = (
    retail_df
    .withColumn(
        "InvoiceType",
        F.when(
            F.col("Invoice").startswith("C"),
            "Cancelled"
        ).otherwise("Normal")
    )
    .groupBy("InvoiceType")
    .count()
    .orderBy(F.desc("count"))
)

invoice_types.show()


# Stop Spark

spark.stop()