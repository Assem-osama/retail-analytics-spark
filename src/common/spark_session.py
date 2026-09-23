from pyspark.sql import SparkSession


def create_spark_session():
    return (
        SparkSession.builder
        .appName("OnlineRetail-Spark-Pipeline")
        .master("spark://spark-master:7077")
        .config("spark.executor.cores", "1")
        .config("spark.executor.memory", "512m")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )