import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark_template.models.user import get_user_schema


@pytest.fixture(scope="module")
def users_df(spark: SparkSession) -> DataFrame:
    """
    Get user dataframe.
    :param spark: SparkSession
    :return: User DataFrame
    """
    data = [
        (1, "Alice", True, None, None, None, None, None, 21),
        (2, "Bob", False, None, None, None, None, None, 17),
        (3, "Charlie", True, None, None, None, None, None, 64)
    ]
    input_df = spark.createDataFrame(data, get_user_schema())

    return input_df

