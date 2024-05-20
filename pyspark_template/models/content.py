from pydantic import BaseModel
from typing import Optional
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def get_content_schema() -> StructType:
    return StructType([
        StructField('content_id', IntegerType()),
        StructField('title', StringType()),
        StructField('rating', StringType()),
        StructField('running_time', IntegerType())
    ])


class Content(BaseModel):
    content_id: int
    title: str
    rating: str
    running_time: int

