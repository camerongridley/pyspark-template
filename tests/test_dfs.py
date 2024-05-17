from pyspark.sql import SparkSession, DataFrame
from pyspark_template.models.user import User
from pyspark_template.transformations import filtering
from tests.fixtures.data import df
from tests.fixtures.spark_session import spark


def test_active_user_filtering(spark: SparkSession, df: DataFrame):
    result_df = transformations.filter_active_users(df)

    # Collect results and validate using Pydantic
    results = result_df.collect()
    for row in results:
        user = User(user_id=row.user_id, username=row.username, is_active=row.is_active)
        assert user.is_active == True  # Validate that only active users are in the result

