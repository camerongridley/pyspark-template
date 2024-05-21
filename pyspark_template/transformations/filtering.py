from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def filter_active_users(input_df: DataFrame) -> DataFrame:
    """
    Filter active users from the dataframe.
    :param input_df: DataFrame to filter
    :return: Filtered DataFrame with only active users
    """
    return input_df.filter(col("is_active") == True)

