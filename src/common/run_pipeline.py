import logging
from common.spark_session import create_spark_session

from bronze.load_bronze import run_bronze
from silver.transform_silver import run_silver
from gold.build_gold_marts import run_gold
from gold.validate_gold import validate_gold 

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

def main():

    print("======================================")
    print("ONLINE RETAIL SPARK PIPELINE")
    print("======================================")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:

        # Bronze
        print("\n>>> Starting Bronze layer...")
        bronze_rows = run_bronze(spark)
        print(f"Bronze completed: {bronze_rows:,} rows")

        # Silver
        print("\n>>> Starting Silver layer...")
        silver_rows = run_silver(spark)
        print(f"Silver completed: {silver_rows:,} rows")

        # Gold
        print("\n>>> Starting Gold layer...")
        gold_rows = run_gold(spark)
        print(f"Gold completed: {gold_rows:,} rows (Daily Sales)")

        # Validation
        print("\n>>> Starting Gold Validation...")
        validate_rows = validate_gold(spark)
        print(f"Validation completed successfully. Checked {validate_rows:,} Daily Sales rows.")
        

        print("\n======================================")
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print("======================================")

    except Exception as e:
        
        logger.exception("PIPELINE FAILED")
        raise

    finally:

        spark.stop()


if __name__ == "__main__":
    main()