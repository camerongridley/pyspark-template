from pydantic import BaseModel
from typing import Optional

class User(BaseModel):
    user_id: int
    username: str
    is_active: bool
    address: str
    city: str
    state: str
    area_code: int
    country: str
    age: Optional [int]

