import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType


@pytest.fixture()
def df(spark):
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

    return input_df