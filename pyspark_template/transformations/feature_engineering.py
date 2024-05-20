from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def create_age_group(df) -> DataFrame:
    return df.withColumn('age_group', col('age').cast('integer')/10)