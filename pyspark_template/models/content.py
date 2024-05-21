from pydantic import BaseModel
from typing import Optional
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def get_content_schema() -> StructType:
    """
    Get content schema StructType
    :return: StructType for content
    """
    return StructType([
        StructField('content_id', IntegerType()),
        StructField('title', StringType()),
        StructField('rating', StringType()),
        StructField('running_time', IntegerType())
    ])


class Content(BaseModel):
    """
    content_id: int - unique id for each content
    title: str - title of the content
    rating: str - rating of the content
    running_time: int - running time of the content
    """
    content_id: int
    title: str
    rating: str
    running_time: int

