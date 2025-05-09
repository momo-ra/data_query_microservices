from pydantic import BaseModel, Field
from datetime import datetime
from typing import List, Optional, Generic, TypeVar

class TagSchema(BaseModel):
    """Schema for Tag"""
    id: str = Field(..., description="Tag ID")
    name: str = Field(..., description="Tag Name")
    description: str = Field(..., description="Tag Description")
    timestamp: str = Field(..., description="Tag Timestamp")
    value: str = Field(..., description="Tag Value")
    unit_of_measure: str = Field(..., description="Tag Unit of Measure")
    
class WebSocketCardSchema(BaseModel):
    """Schema for WebSocket card messages"""
    card_id: str = Field(..., description="Card ID")
    tag: TagSchema
    graph_type: str = Field(..., description="Graph Type")

class CreateCardSchema(BaseModel):
    user_id: int = Field(..., description="User Id")
    start_time: datetime = Field(..., description="Starting date for historical data")
    end_time: str = Field(default='now', description="Ending data it could Be Now")
    is_active: bool = Field(default=True)
    tags: List[int] = Field(..., description="It's array of tags that user interested about it")

# Generic type for response data
T = TypeVar('T')

class StandardResponse(BaseModel, Generic[T]):
    """Base model for standardized API responses"""
    status: str
    data: Optional[T] = None
    message: Optional[str] = None

class TagListResponse(StandardResponse[List[TagSchema]]):
    """Standard response with a list of validated tags"""
    data: List[TagSchema]

class CardSchema(BaseModel):
    card_id: str = Field(..., description="Card ID")
    tag: TagSchema
    graph_type: str = Field(..., description="Graph Type")

class CardListResponse(StandardResponse[List[CardSchema]]):
    data: List[CardSchema]

class GraphSchema(BaseModel):
    id: int = Field(..., description='Unique Field for Graph Type')
    name: str = Field(..., description='Graph Type Field')
    description: Optional[str] = Field(description='Graph Type Description')
