from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    """
    Get or create spark session for pyspark
    :return: SparkSession
    """
    spark = SparkSession.builder \
        .appName("pyspark template") \
        .master('local') \
        .getOrCreate()

    return spark

