from pydantic import BaseModel
from typing import Optional
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType


def get_user_schema():
    return StructType([
        StructField("user_id", IntegerType(), True),
        StructField("username", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("area_code", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("age", IntegerType(), True)
    ])


class User(BaseModel):
    user_id: int
    username: str
    is_active: bool
    address: Optional [str] = None
    city: Optional [str] = None
    state: Optional [str] = None
    area_code: Optional [int] = None
    country: Optional [str] = None
    age: Optional [int] = None
