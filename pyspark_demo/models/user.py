from pydantic import BaseModel
from typing import Optional

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

