from pydantic import BaseModel
from datetime import datetime

class Activity(BaseModel):
    activity_id: int
    user_id: int
    content_id: int
    latest_date_watched: datetime
    latest_running_time: int
