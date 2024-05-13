import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Fixture for creating a Spark session.
    """
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("PySparkTest") \
        .getOrCreate()

    yield spark

    spark.stop()