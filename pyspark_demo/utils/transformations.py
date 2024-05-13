from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def filter_active_users(input_df: DataFrame) -> DataFrame:
    """Filter active users from the input DataFrame."""
    filtered_df = input_df.filter(col("is_active") == True)
    return filtered_df
