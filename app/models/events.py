from pydantic import BaseModel, Field
from uuid import UUID
from datetime import datetime
from typing import Dict, Any, List

class Event(BaseModel):
    event_id: UUID
    occurred_at: datetime
    user_id: str
    event_type: str
    properties: Dict[str, Any] = Field(default_factory=dict)

class EventBatch(BaseModel):
    events: List[Event]

class DAUResponse(BaseModel):
    date: str
    unique_users: int

class TopEventResponse(BaseModel):
    event_type: str
    count: int

class RetentionResponse(BaseModel):
    cohort_week: str
    week_0: int
    week_1: float
    week_2: float
    week_3: float