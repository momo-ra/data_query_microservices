from sqlalchemy import Column, Integer, Boolean, DateTime, ForeignKey, Table, String, Table
from sqlalchemy.orm import declarative_base, relationship


Base = declarative_base()

tag = Table(
    'tag',
    Base.metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String, nullable=False),
    Column('description', String, nullable=False),
    Column('created_at', DateTime, nullable=False),
    Column('updated_at', DateTime, nullable=False)
)
# Association table for many-to-many relationship between CardData and Tag
card_data_tags = Table(
    'card_data_tags',
    Base.metadata,
    Column('card_data_id', Integer, ForeignKey('card_data.id'), primary_key=True),
    Column('tag_id', Integer, ForeignKey('tag.id'), primary_key=True)
)

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
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)

class Role(Base):
    """
    Represents a role in the database.
    """
    __tablename__ = "role"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)

    # Relationships
    users = relationship("User", back_populates="role")

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
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)

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
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)

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
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    
    # Relationships
    tags = relationship("Tag", secondary=card_data_tags, back_populates="cards")
    user = relationship("User", back_populates="cards")

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
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    
    # Relationships
    cards = relationship("CardData", back_populates="graph_type")
    
    def __repr__(self):
        return f"<GraphType(id={self.id}, name={self.name}, description={self.description})>"
    
    
    