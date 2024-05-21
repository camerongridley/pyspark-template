from pyspark.sql import DataFrame, SparkSession
from pyspark_template.transformations import filtering, feature_engineering
from tests.fixtures.data import users_df
from tests.fixtures.spark_session import spark # needed for users_df fixture
from chispa import assert_df_equality


def test_active_user_filtering(users_df: DataFrame):
    result_df = filtering.filter_active_users(users_df)
    result_df.show()
    # Collect results and validate using Pydantic
    results = result_df.collect()
    for row in results:
        assert(row.is_active == True)


def test_age_group(spark: SparkSession, users_df: DataFrame):
    result_df = feature_engineering.create_age_group(users_df)
    # add expected values to the df fixture
    age_grp_data = [(1,2), (2,1), (3,6)]
    cols = ['user_id', 'age_group']
    age_grp_df = spark.createDataFrame(age_grp_data, cols)
    # create new df with expected age groups
    expected_df = users_df.join(age_grp_df, 'user_id')
    expected_df.show()
    assert_df_equality(result_df, expected_df, ignore_row_order=True)
