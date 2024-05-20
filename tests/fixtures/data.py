import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
)
from pyspark_template.models.user import User, user_schema

#
# @pytest.fixture()
# def users_df(spark):
#     # Define schema for the input data
#     schema = StructType([
#         StructField("user_id", IntegerType(), True),
#         StructField("username", StringType(), True),
#         StructField("is_active", BooleanType(), True),
#         StructField("age", IntegerType(), True)
#     ])
#
#     data = [
#         (1, "Alice", True, 21),
#         (2, "Bob", False, 17),
#         (3, "Charlie", True, 64)
#     ]
#     input_df = spark.createDataFrame(data, schema)
#
#     return input_df


@pytest.fixture()
def users_df(spark) -> DataFrame:
    users = [
        User(
            user_id=1,
            username="Alice",
            is_active=True,
            age=33,
        ),
        User(
            user_id=2,
            username="Bob",
            is_active=False,
            age=56,
        ),
        User(
            user_id=3,
            username="Gary",
            is_active=True,
            age=19,
        ),
    ]

    user_dicts = [u.dict() for u in users]

    return spark.createDataFrame(user_dicts, user_schema())


