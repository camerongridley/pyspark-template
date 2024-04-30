from pydantic import BaseModel
from typing import Optional

class Content(BaseModel):
    content_id: int
    title: str
    rating: str
    running_time: int