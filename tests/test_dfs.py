import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark_demo.models.user import User
from pyspark.sql.functions import col


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


def filter_active_users(input_df: DataFrame) -> DataFrame:
    """Filter active users from the input DataFrame."""
    filtered_df = input_df.filter(col("is_active") == True)
    return filtered_df


def test_active_user_filtering(spark: SparkSession):
    # Define schema for the input data
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("username", StringType(), True),
        StructField("is_active", BooleanType(), True)
    ])

    data = [
        (1, "Alice", True),
        (2, "Bob", False),
        (3, "Charlie", True)
    ]
    input_df = spark.createDataFrame(data, schema)

    result_df = filter_active_users(input_df)

    # Collect results and validate using Pydantic
    results = result_df.collect()
    for row in results:
        user = User(user_id=row.user_id, username=row.username, is_active=row.is_active)
        assert user.is_active == True  # Validate that only active users are in the result

