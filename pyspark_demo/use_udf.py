from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lit
from pyspark_demo.utils.udfs import mult_col, handle_null_example


spark = SparkSession.builder \
    .appName('pyspark template') \
    .getOrCreate()

spark.udf.register('mult_col', mult_col, IntegerType())

data = [(1,), (None,), (3,)]
columns = ["number"]
df = spark.createDataFrame(data, columns)

factor=5

df_mult = df.withColumn("num_mult", mult_col(df['number'], lit(factor)))
df_mult.show()

spark.udf.register('add_col', handle_null_example, IntegerType())
df_add = df.withColumn('add_col', handle_null_example(df['number'], lit(500)))
df_add.show()