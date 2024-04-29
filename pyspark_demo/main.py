from pyspark_demo.utils import math_ops
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

print('1 + 2 =', math_ops.add(1, 2))

spark = SparkSession.builder \
    .appName('pyspark template') \
    .getOrCreate()

# Define schema for the DataFrame
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

# Data to be included in the DataFrame
data = [
    ("John Doe", 30, "New York"),
    ("Jane Doe", 25, "Los Angeles"),
    ("Mike Johnson", 35, "Chicago")
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

df.show()

# Showing different syntax for filtering
df2 = df.filter(df.age >= 30)
df2.show()
df3 = df.filter('age == 35')
df3.show()
df4 = df.filter(col('age') == 30)
df4.show()

data = [("James", "Sales", 3000),
        ("Michael", "Sales", 4600),
        ("Robert", "Sales", 4100),
        ("Maria", "Finance", 3000),
        ("James", "Sales", 3000),
        ("Scott", "Finance", 3300),
        ("Jen", "Finance", 3900),
        ("Jeff", "Marketing", 3000),
        ("Kumar", "Marketing", 2000),
        ("Saif", "Sales", 4100)]
columns = ["Employee_Name", "Department", "Salary"]

df = spark.createDataFrame(data, columns)
df_filtered = df.filter(col("Salary") > 3000)
df_filtered.show()

