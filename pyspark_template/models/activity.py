from pydantic import BaseModel
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, DateType


def get_activity_schema() -> StructType:
    return StructType([
        StructField('activity_id', IntegerType()),
        StructField('user_id', IntegerType()),
        StructField('content_id', IntegerType()),
        StructField('latest_date_watched', DateType()),
        StructField('latest_running_time', IntegerType())
    ])


class Activity(BaseModel):
    activity_id: int
    user_id: int
    content_id: int
    latest_date_watched: datetime
    latest_running_time: int


