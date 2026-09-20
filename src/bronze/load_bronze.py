from pyspark.sql import SparkSession
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
    .appName("OnlineRetailII-Bronze")
    .master("spark://spark-master:7077")
    .config("spark.executor.cores", "1")
    .config("spark.executor.memory", "512m")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

RAW_DATA_PATH = "/data/raw/online_retail_II.csv"
BRONZE_DATA_PATH = "/data/bronze/online_retail"


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

print("BRONZE INGESTION STARTED")

retail_df = (
    spark.read
    .option("header", True)
    .schema(retail_schema)
    .csv(RAW_DATA_PATH)
)

print("\nSource Data:")
print(f"Rows: {retail_df.count():,}")
print(f"Columns: {len(retail_df.columns)}")


# Write Bronze as Parquet
print("\nWriting Bronze data...")

(
    retail_df.write
    .mode("overwrite")
    .parquet(BRONZE_DATA_PATH)
)

# Validation

bronze_df = spark.read.parquet(BRONZE_DATA_PATH)

bronze_count = bronze_df.count()

print("\n" + "=" * 80)
print("BRONZE VALIDATION")
print("=" * 80)

print(f"Source Rows:  {retail_df.count():,}")
print(f"Bronze Rows:  {bronze_count:,}")
print(f"Columns:      {len(bronze_df.columns)}")

print("\nBronze Schema:")
bronze_df.printSchema()

print("\nBronze Sample:")
bronze_df.show(5, truncate=False)

# Finish
print("BRONZE INGESTION COMPLETED")
spark.stop()

