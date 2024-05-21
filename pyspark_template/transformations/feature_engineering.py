from pyspark.sql import DataFrame
from pyspark.sql.functions import col, floor

def create_age_group(df:DataFrame) -> DataFrame:
    """
    Create age group feature. Groups are 10 years each, so the age will be floor divided by 10.
    For example, if the age is 25, the age group will be 2.
    :param df:
    :return: DataFrame with new age group column
    """
    return df.withColumn('age_group', floor(col('age').cast('integer')/10))