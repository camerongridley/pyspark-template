from pyspark.sql import SparkSession, DataFrame
from pyspark_template.models.user import User
from pyspark_template.transformations import filtering, feature_engineering
from tests.fixtures.data import users_df
from tests.fixtures.spark_session import spark
from pyspark.sql.functions import col
from chispa import assert_df_equality


def test_active_user_filtering(spark: SparkSession, users_df: DataFrame):
    result_df = filtering.filter_active_users(users_df)

    # Collect results and validate using Pydantic
    results = result_df.collect()
    for row in results:
        user = User(user_id=row.user_id, username=row.username, is_active=row.is_active)
        assert user.is_active == True  # Validate that only active users are in the result

def test_age_group(spark: SparkSession, users_df: DataFrame):
    result_df = feature_engineering.create_age_group(users_df)

    # this
    expected_df = users_df.withColumn('age_group', col('age').cast('integer')/10)

    assert_df_equality(result_df, expected_df, ignore_row_order=True)

