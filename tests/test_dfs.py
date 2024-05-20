from pyspark.sql import DataFrame
from pyspark_template.transformations import filtering, feature_engineering
from tests.fixtures.data import users_df
from tests.fixtures.spark_session import spark # needed for users_df fixture
from pyspark.sql.functions import col
from chispa import assert_df_equality


def test_active_user_filtering(users_df: DataFrame):
    result_df = filtering.filter_active_users(users_df)
    result_df.show()
    # Collect results and validate using Pydantic
    results = result_df.collect()
    for row in results:
        print(row)
        assert(row.is_active == True)


def test_age_group(users_df: DataFrame):
    result_df = feature_engineering.create_age_group(users_df)

    expected_df = users_df.withColumn('age_group', col('age').cast('integer') / 10)

    assert_df_equality(result_df, expected_df, ignore_row_order=True)
