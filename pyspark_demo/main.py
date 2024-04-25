from utils import math
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import lit

print(math.add(1, 2))

spark = SparkSession.builder \
    .appName('pyspark template') \
    .getOrCreate()

# Define schema for the DataFrame
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("City", StringType(), True)
])

# Data to be included in the DataFrame
data = [
    ("John Doe", 30, "New York"),
    ("Jane Doe", 25, "Los Angeles"),
    ("Mike Johnson", 35, "Chicago")
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Display the DataFrame
df.show()


def mult_col(x, factor: int) -> int:
    if x is None:
        return 100
    else:
        return x * factor


spark.udf.register('mult', mult_col, IntegerType())

data = [(1,), (None,), (3,)]
columns = ["number"]
df = spark.createDataFrame(data, columns)

factor=5

df_mult = df.withColumn("num_mult", mult_col(df['number'], lit(factor)))
df_mult.show()

