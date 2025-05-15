from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, UniqueConstraint, PrimaryKeyConstraint, Index, Table, Boolean, Text
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declared_attr
from typing import Optional, List, Dict, Any


Base = declarative_base()
# Association table for many-to-many relationship between CardData and Tag
card_data_tags = Table(
    'card_data_tags',
    Base.metadata,
    Column('card_data_id', Integer, ForeignKey('card_data.id'), primary_key=True),
    Column('tag_id', Integer, ForeignKey('tag.id'), primary_key=True)
)

class BaseModel(object):
    """Base model with common attributes and methods"""
    
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary"""
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

class Tag(Base):
    __tablename__ = "tag"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    description = Column(String, nullable=True)
    unit_of_measure = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
    
    # Relationships
    subscriptions = relationship("subscription_tasks", back_populates="tag")
    cards = relationship("CardData", secondary=card_data_tags, back_populates="tags")
    time_series = relationship("TimeSeries", back_populates="tag")
    alerts = relationship("Alerts", back_populates="tag")
    polling_tasks = relationship("polling_tasks", back_populates="tag")

class TimeSeries(Base):
    __tablename__ = "time_series"
    # Composite primary key instead of id column
    tag_id = Column(Integer, ForeignKey("tag.id"), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    value = Column(String, nullable=False)
    frequency = Column(String, nullable=False)

    # Composite primary key of tag_id and timestamp
    __table_args__ = (
        PrimaryKeyConstraint('tag_id', 'timestamp'),
        Index('idx_time_series_tag_time', 'tag_id', 'timestamp', unique=True),
        Index('idx_time_series_frequency', 'frequency', 'timestamp'),
    )
    
    # Relationship
    tag = relationship("Tag", back_populates="time_series")

class User(Base):
    """
    Represents a user in the database.
    """
    __tablename__ = "user"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    email = Column(String, nullable=False, unique=True)
    role_id = Column(Integer, ForeignKey('role.id'), nullable=False)
    password = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
    
    # Relationships
    role = relationship("Role", back_populates="users")
    cards = relationship("CardData", back_populates="user")
    
    def __repr__(self):
        return f"<User(id={self.id}, name={self.name}, email={self.email})>"

class Role(Base):
    """
    Represents a role in the database.
    """
    __tablename__ = "role"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)

    # Relationships
    users = relationship("User", back_populates="role")
    permissions = relationship("RolePermission", back_populates="role")

    def __repr__(self):
        return f"<Role(id={self.id}, name={self.name}, description={self.description})>"

class Permission(Base):
    """
    Represents a permission in the database.
    """
    __tablename__ = "permission"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)

    # Relationships
    roles = relationship("RolePermission", back_populates="permission")

    def __repr__(self):
        return f"<Permission(id={self.id}, name={self.name}, description={self.description})>"

class RolePermission(Base):
    """
    Represents a role permission in the database.
    """
    __tablename__ = "role_permission"

    id = Column(Integer, primary_key=True)
    role_id = Column(Integer, ForeignKey('role.id'), nullable=False)
    permission_id = Column(Integer, ForeignKey('permission.id'), nullable=False)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)

    # Relationships
    role = relationship("Role", back_populates="permissions")
    permission = relationship("Permission", back_populates="roles")

class CardData(Base):
    """
    Represents a card data record in the database.
    """
    __tablename__ = "card_data"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('user.id'), nullable=False)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=False)
    is_active = Column(Boolean, nullable=False)
    graph_type_id = Column(Integer, ForeignKey('graph_type.id'), nullable=False)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
    
    # Relationships
    tags = relationship("Tag", secondary=card_data_tags, back_populates="cards")
    user = relationship("User", back_populates="cards")
    graph_type = relationship("GraphType", back_populates="cards")

    def __repr__(self):
        return f"<CardData(id={self.id}, user_id={self.user_id}, start_time={self.start_time}, end_time={self.end_time}, is_active={self.is_active})>"

class GraphType(Base):
    """
    Represents a graph type in the database.
    """
    __tablename__ = "graph_type"
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
    
    # Relationships
    cards = relationship("CardData", back_populates="graph_type")
    
    def __repr__(self):
        return f"<GraphType(id={self.id}, name={self.name}, description={self.description})>"
    
class Alerts(Base):
    __tablename__ = "alerts"
    id = Column(Integer, primary_key=True, autoincrement=True)
    tag_id = Column(Integer, ForeignKey("tag.id"), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    message = Column(String, nullable=False)
    severity = Column(String, nullable=True)
    is_acknowledged = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    
    # Relationship
    tag = relationship("Tag", back_populates="alerts")
    
    def __repr__(self):
        return f"<Alert(id={self.id}, tag_id={self.tag_id}, timestamp={self.timestamp})>"

class polling_tasks(Base):
    __tablename__ = "polling_tasks"
    id = Column(Integer, primary_key=True, autoincrement=True)
    tag_id = Column(Integer, ForeignKey("tag.id"), nullable=False)
    time_interval = Column(Integer, nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)
    last_polled = Column(DateTime, nullable=True)
    next_polled = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
    
    __table_args__ = (
        UniqueConstraint('tag_id', 'time_interval'),
    )
    
    # Relationship
    tag = relationship("Tag", back_populates="polling_tasks")
    
    def __repr__(self):
        return f"<polling_tasks(id={self.id}, tag_id={self.tag_id}, interval={self.time_interval})>"

class subscription_tasks(Base):
    """Model for storing OPC UA subscription tasks"""
    __tablename__ = "subscription_tasks"
    
    id = Column(Integer, primary_key=True, index=True)
    tag_id = Column(Integer, ForeignKey("tag.id"))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now)
    last_updated = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # Relationship
    tag = relationship("Tag", back_populates="subscriptions")
    
    def __repr__(self):
        return f"<subscription_tasks(id={self.id}, tag_id={self.tag_id}, is_active={self.is_active})>"

class ChatSession(Base):
    __tablename__ = 'chat_sessions'
    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(String, unique=True, nullable=False)
    user_id = Column(Integer, ForeignKey('user.id'), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    messages = relationship("ChatMessage", back_populates="session", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<ChatSession(id={self.id}, session_id={self.session_id})>"

class ChatMessage(Base):
    __tablename__ = 'chat_messages'
    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(String, ForeignKey('chat_sessions.session_id'), nullable=False)
    message = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    query = Column(String, nullable=True)
    response = Column(String, nullable=True)
    execution_time = Column(Float, nullable=True)
    is_from_user = Column(Boolean, default=True)
    
    # Relationship
    session = relationship("ChatSession", back_populates="messages")
    
    def __repr__(self):
        return f"<ChatMessage(id={self.id}, session_id={self.session_id})>"

class MathOperationModel(Base):
    __tablename__ = "math_operation"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    operator = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)
    __table_args__ = (
        UniqueConstraint('name', 'operator', name="uq_math_operation_name_operator"),
    )


class AlertingFormula(Base):
    __tablename__ = "alerting_formula"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)  # comparison between tag_1 and tag_2 or comparison between one tag and a threshold value
    tag_1 = Column(Integer, ForeignKey("tag.id"), nullable=False)
    tag_2 = Column(Integer, ForeignKey("tag.id"), nullable=True)  # Should be nullable if comparing with threshold
    math_operation_id = Column(Integer, ForeignKey("math_operation.id"), nullable=False)
    threshold = Column(Float, nullable=True)  # value to compare with tag_1 or tag_2
    # description = Column(Text, nullable=True)
    bucket_size = Column(Integer, nullable=False)
    time_window = Column(Integer, nullable=False)  # in seconds
    is_active = Column(Boolean, default=True)
    frequency = Column(Integer, nullable=False)  # in seconds  
    last_check_time = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)

class AlertingData(Base):
    __tablename__ = "alerting_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    formula_id = Column(Integer, ForeignKey("alerting_formula.id"), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    message = Column(Text, nullable=True)
    start_condition_time = Column(DateTime, nullable=True)
    end_condition_time = Column(DateTime, nullable=True)
    fingerprint = Column(String(64), nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)
