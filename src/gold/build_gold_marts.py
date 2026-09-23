from pyspark.sql.functions import (
    col,
    to_date,
    year,
    month,
    date_format,
    countDistinct,
    sum,
    abs,
    round,
    when,
    lit,
    coalesce,
    avg,
    row_number,
    desc,
    first
)
from pyspark.sql.window import Window

def run_gold(spark):

    SILVER_DATA_PATH = "/data/silver/online_retail"

    GOLD_DAILY_SALES_PATH = "/data/gold/daily_sales"
    GOLD_PRODUCT_PATH = "/data/gold/product_performance"
    GOLD_CUSTOMER_PATH = "/data/gold/customer"
    GOLD_COUNTRY_PATH = "/data/gold/country_sales"


    print("GOLD DATA MARTS BUILD STARTED")


    # 1. READ SILVER

    silver_df = spark.read.parquet(SILVER_DATA_PATH)

    print("\nSilver Data:")
    print(f"Rows: {silver_df.count():,}")
    print(f"Columns: {len(silver_df.columns)}")

    print("\nSilver Schema:")
    silver_df.printSchema()

    print("\nSilver Partitions:")
    print(silver_df.rdd.getNumPartitions())


    # 2. PREPARE COMMON COLUMNS

    gold_base_df = (
        silver_df
        .withColumn(
            "SaleDate",
            to_date(col("InvoiceDate"))
        )
        .withColumn(
            "Year",
            year(col("InvoiceDate"))
        )
        .withColumn(
            "Month",
            month(col("InvoiceDate"))
        )
        .withColumn(
            "MonthName",
            date_format(col("InvoiceDate"), "MMMM")
        )
    )


    # 3. BUSINESS METRICS

    gold_base_df = (
        gold_base_df

        .withColumn(
            "GrossSales",
            when(
                (col("TransactionType") == "Normal") &
                (col("Quantity") > 0),
                col("Revenue")
            ).otherwise(0.0)
        )

        .withColumn(
            "ReturnAmount",
            when(
                (col("TransactionType") == "Normal") &
                (col("Quantity") < 0),
                abs(col("Revenue"))
            ).otherwise(0.0)
        )

        .withColumn(
            "CancelledAmount",
            when(
                col("TransactionType") == "Cancelled",
                abs(col("Revenue"))
            ).otherwise(0.0)
        )

        .withColumn(
            "ReturnUnits",
            when(
                (col("TransactionType") == "Normal") &
                (col("Quantity") < 0),
                abs(col("Quantity"))
            ).otherwise(0)
        )

        .withColumn(
            "SoldUnits",
            when(
                (col("TransactionType") == "Normal") &
                (col("Quantity") > 0),
                col("Quantity")
            ).otherwise(0)
        )
    )


    # 4. DAILY SALES DATA MART

    print("BUILDING DAILY SALES MART")

    daily_sales_mart = (
        gold_base_df
        .groupBy(
            "SaleDate",
            "Year",
            "Month",
            "MonthName",
            "Country"
        )
        .agg(
            countDistinct("Invoice").alias("Orders"),

            countDistinct(
                when(
                    col("Customer ID").isNotNull(),
                    col("Customer ID")
                )
            ).alias("Customers"),

            sum("SoldUnits").alias("UnitsSold"),

            sum("ReturnUnits").alias("ReturnUnits"),

            sum("GrossSales").alias("GrossSales"),

            sum("ReturnAmount").alias("ReturnAmount"),

            sum("CancelledAmount").alias("CancelledAmount")
        )
        .withColumn(
            "NetRevenue",
            col("GrossSales") - col("ReturnAmount") - col("CancelledAmount")
        )
        .select(
            "SaleDate",
            "Year",
            "Month",
            "MonthName",
            "Country",
            "Orders",
            "Customers",
            "UnitsSold",
            "ReturnUnits",
            "GrossSales",
            "ReturnAmount",
            "CancelledAmount",
            "NetRevenue"
        )
    )


    # 5. PRODUCT PERFORMANCE DATA MART

    print("BUILDING PRODUCT PERFORMANCE MART")

    product_performance_mart = (
        gold_base_df
        .groupBy("StockCode")
        .agg(
            first("Description", ignorenulls=True).alias("Description"),
            countDistinct("Invoice").alias("Orders"),
            sum("SoldUnits").alias("UnitsSold"),
            sum("ReturnUnits").alias("ReturnUnits"),
            sum("GrossSales").alias("GrossSales"),
            sum("ReturnAmount").alias("ReturnAmount"),
            sum("CancelledAmount").alias("CancelledAmount")
        )
        .withColumn(
            "NetRevenue",
            col("GrossSales") - col("ReturnAmount") - col("CancelledAmount")
        )
        .withColumn(
            "ReturnRate",
            when(
                col("UnitsSold") > 0,
                round(col("ReturnUnits") / col("UnitsSold") * 100, 2)
            ).otherwise(0.0)
        )
    )


    # 6. PRODUCT REVENUE RANK

    product_rank_window = Window.orderBy(desc("NetRevenue"))

    product_performance_mart = (
        product_performance_mart
        .withColumn(
            "RevenueRank",
            row_number().over(product_rank_window)
        )
    )


    # 7. CUSTOMER DATA MART

    print("BUILDING CUSTOMER MART")

    customer_base_df = gold_base_df.filter(
        col("Customer ID").isNotNull()
    )


    customer_mart = (
        customer_base_df
        .groupBy(
            "Customer ID",
            "Country"
        )
        .agg(
            countDistinct("Invoice").alias("Orders"),

            sum("SoldUnits").alias("UnitsPurchased"),

            sum("ReturnUnits").alias("ReturnUnits"),

            sum("GrossSales").alias("GrossSales"),

            sum("ReturnAmount").alias("ReturnAmount"),

            sum("CancelledAmount").alias("CancelledAmount")
        )
        .withColumn(
            "NetRevenue",
            col("GrossSales") - col("ReturnAmount") - col("CancelledAmount")
        )
        .withColumn(
            "AverageOrderValue",
            when(
                col("Orders") > 0,
                round(
                    col("GrossSales") / col("Orders"),
                    2
                )
            ).otherwise(0.0)
        )
    )


    # =============================================================================
    # 8. CUSTOMER REVENUE RANK
    # =============================================================================

    customer_rank_window = Window.orderBy(
        desc("NetRevenue")
    )

    customer_mart = (
        customer_mart
        .withColumn(
            "CustomerRank",
            row_number().over(customer_rank_window)
        )
    )


    # =============================================================================
    # 9. COUNTRY SALES DATA MART
    # =============================================================================

    print("\n" + "=" * 80)
    print("BUILDING COUNTRY SALES MART")
    print("=" * 80)

    country_sales_mart = (
        gold_base_df
        .groupBy("Country")
        .agg(
            countDistinct("Invoice").alias("Orders"),

            countDistinct(
                when(
                    col("Customer ID").isNotNull(),
                    col("Customer ID")
                )
            ).alias("Customers"),

            sum("SoldUnits").alias("UnitsSold"),

            sum("ReturnUnits").alias("ReturnUnits"),

            sum("GrossSales").alias("GrossSales"),

            sum("ReturnAmount").alias("ReturnAmount"),

            sum("CancelledAmount").alias("CancelledAmount")
        )
        .withColumn(
            "NetRevenue",
            col("GrossSales") - col("ReturnAmount") - col("CancelledAmount")
        )
    )


    # 10. COUNTRY REVENUE RANK

    country_rank_window = Window.orderBy(desc("NetRevenue"))

    country_sales_mart = (
        country_sales_mart
        .withColumn(
            "CountryRank",
            row_number().over(country_rank_window)
        )
    )


    # 11. ROUND FINANCIAL COLUMNS

    financial_columns = [
        "GrossSales",
        "ReturnAmount",
        "CancelledAmount",
        "NetRevenue"
    ]


    for dataframe_name, dataframe in [
        ("Daily Sales", daily_sales_mart),
        ("Product Performance", product_performance_mart),
        ("Customer", customer_mart),
        ("Country Sales", country_sales_mart)
    ]:

        for column_name in financial_columns:

            if column_name in dataframe.columns:

                dataframe = dataframe.withColumn(
                    column_name,
                    round(col(column_name), 2)
                )

        if dataframe_name == "Daily Sales":
            daily_sales_mart = dataframe

        elif dataframe_name == "Product Performance":
            product_performance_mart = dataframe

        elif dataframe_name == "Customer":
            customer_mart = dataframe

        elif dataframe_name == "Country Sales":
            country_sales_mart = dataframe


    # 12. WRITE DATA MARTS

    print("WRITING GOLD DATA MARTS")

    (
        daily_sales_mart
        .write
        .mode("overwrite")
        .parquet(GOLD_DAILY_SALES_PATH)
    )


    (
        product_performance_mart
        .write
        .mode("overwrite")
        .parquet(GOLD_PRODUCT_PATH)
    )


    (
        customer_mart
        .write
        .mode("overwrite")
        .parquet(GOLD_CUSTOMER_PATH)
    )


    (
        country_sales_mart
        .write
        .mode("overwrite")
        .parquet(GOLD_COUNTRY_PATH)
    )


    # 13. DATA MART VALIDATION

    print("\n" + "=" * 80)
    print("GOLD DATA MART VALIDATION")
    print("=" * 80)


    print("\nDaily Sales Mart:")
    daily_sales_check = spark.read.parquet(
        GOLD_DAILY_SALES_PATH
    )

    print(f"Rows: {daily_sales_check.count():,}")

    daily_sales_check.show(10,truncate=False)


    print("\nProduct Performance Mart:")
    product_check = spark.read.parquet(GOLD_PRODUCT_PATH)

    print(f"Rows: {product_check.count():,}")

    product_check.orderBy(
        "RevenueRank"
    ).show(
        10,
        truncate=False
    )


    print("\nCustomer Mart:")
    customer_check = spark.read.parquet(GOLD_CUSTOMER_PATH)

    print(f"Rows: {customer_check.count():,}")

    customer_check.orderBy("CustomerRank").show(10,truncate=False)


    print("\nCountry Sales Mart:")
    country_check = spark.read.parquet(
        GOLD_COUNTRY_PATH
    )

    print(f"Rows: {country_check.count():,}")

    country_check.orderBy("CountryRank").show(10,truncate=False)


    # 14. FINAL
    print("GOLD DATA MARTS BUILD COMPLETED")


    return daily_sales_check.count()


if __name__ == "__main__":
    from common.spark_session import create_spark_session
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        rows = run_gold(spark)
        print(f"gold completed: {rows:,} rows")

    finally:
        spark.stop()
