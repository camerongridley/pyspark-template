from pyspark.sql import SparkSession, DataFrame


spark = SparkSession.builder \
    .appName('pyspark template') \
    .getOrCreate()


def mult_col(x, factor: int) -> int:
    return x * factor

spark.udf.register('mult', mult_col())
spark.range(10).createOrReplaceTempView('test')
spark.sql("SELECT * FROM Otest WHERE mult(id)").show()

