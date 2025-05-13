from pydantic import BaseModel, Field
from datetime import datetime
from typing import List, Optional, Generic, TypeVar, Any, Dict, Union

# Generic type for response data
T = TypeVar('T')

class ResponseModel(BaseModel, Generic[T]):
    """Base model for standardized API responses"""
    status: str = Field(..., description="Status of Response(success/fail)")
    data: Optional[T] = Field(None, description="Data should be return")
    message: Optional[str] = Field(None, description="Error message when fail happened")

# Keeping existing ResponseSchema for backward compatibility
class ResponseSchema(ResponseModel[T], Generic[T]):
    """Alias for ResponseModel for backward compatibility"""
    pass

# Keeping existing StandardResponse for backward compatibility
class StandardResponse(ResponseModel[T], Generic[T]):
    """Alias for ResponseModel for backward compatibility"""
    pass

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

class TagListResponse(ResponseModel[List[TagSchema]]):
    """Standard response with a list of validated tags"""
    data: List[TagSchema]

class CardSchema(BaseModel):
    card_id: str = Field(..., description="Card ID")
    tag: TagSchema
    graph_type: str = Field(..., description="Graph Type")

class CardListResponse(ResponseModel[List[CardSchema]]):
    data: List[CardSchema]

class GraphSchema(BaseModel):
    id: int = Field(..., description='Unique Field for Graph Type')
    name: str = Field(..., description='Graph Type Field')
    description: Optional[str] = Field(None, description='Graph Type Description')
