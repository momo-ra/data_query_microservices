from sqlalchemy.ext.asyncio import AsyncSession
from database import get_db
from utils.log import setup_logger
from fastapi import Depends, HTTPException
from middleware.auth_middleware import authenticate_user, get_user_id, is_admin
from sqlalchemy import text
from typing import List, Dict, Any, Optional, Set, Union
logger = setup_logger(__name__)

# Define common permission names for reuse
class Permissions:
    VIEW_ANY_USER_CARDS = "view_any_user_cards"
    CREATE_ANY_USER_CARDS = "create_any_user_cards"
    DELETE_ANY_USER_CARDS = "delete_any_user_cards"
    EDIT_ANY_USER_CARDS = "edit_any_user_cards"
    ADMIN_ACCESS = "admin_access"
    

async def check_permission(permission_name: str, db: AsyncSession = Depends(get_db), user_id: int = None):
    """
    Check if a user has a specific permission.
    Returns True if permission exists, False otherwise.
    """
    if not user_id:
        logger.warning("No user_id provided to check_permission")
        return False
        
    try:
        query = text("""
            SELECT 1 FROM permission p
            JOIN role_permission rp ON p.id = rp.permission_id
            JOIN role r ON rp.role_id = r.id
            JOIN "user" u ON u.role_id = r.id
            WHERE u.id = :user_id AND p.name = :permission_name
        """)
        result = await db.execute(query, {"user_id": user_id, "permission_name": permission_name})
        has_permission = result.scalar_one_or_none() is not None
        
        if has_permission:
            logger.info(f"User {user_id} has permission: {permission_name}")
        else:
            logger.warning(f"User {user_id} does NOT have permission: {permission_name}")
            
        return has_permission
    except Exception as e:
        logger.error(f"Error checking permission for user {user_id}: {e}")
        return False

async def get_user_permissions(db: AsyncSession, user_id: int) -> List[str]:
    """
    Get all permissions for a specific user.
    Returns a list of permission names.
    """
    try:
        query = text("""
            SELECT p.name FROM permission p
            JOIN role_permission rp ON p.id = rp.permission_id
            JOIN role r ON rp.role_id = r.id
            JOIN "user" u ON u.role_id = r.id
            WHERE u.id = :user_id
        """)
        result = await db.execute(query, {"user_id": user_id})
        permissions = [row[0] for row in result.all()]
        logger.info(f"User {user_id} has permissions: {permissions}")
        return permissions
    except Exception as e:
        logger.error(f"Error fetching permissions for user {user_id}: {e}")
        return []

async def get_user_role(db: AsyncSession, user_id: int) -> Optional[Dict[str, Any]]:
    """
    Get the role information for a specific user.
    Returns a dictionary with role details or None if not found.
    """
    try:
        query = text("""
            SELECT r.id, r.name, r.description FROM role r
            JOIN "user" u ON u.role_id = r.id
            WHERE u.id = :user_id
        """)
        result = await db.execute(query, {"user_id": user_id})
        role_row = result.first()
        
        if not role_row:
            logger.warning(f"No role found for user {user_id}")
            return None
            
        role = {
            "id": role_row[0],
            "name": role_row[1],
            "description": role_row[2]
        }
        logger.info(f"User {user_id} has role: {role['name']}")
        return role
    except Exception as e:
        logger.error(f"Error fetching role for user {user_id}: {e}")
        return None

# FastAPI dependency for requiring specific permissions
class RequirePermission:
    """
    FastAPI dependency for requiring specific permissions.
    """
    def __init__(self, permission_name: str):
        self.permission_name = permission_name
        
    async def __call__(
        self, 
        db: AsyncSession = Depends(get_db), 
        auth_data: Dict[str, Any] = Depends(authenticate_user)
    ) -> Dict[str, Any]:
        user_id = get_user_id(auth_data)
        
        # Always allow admins to bypass permission checks
        if is_admin(auth_data):
            return auth_data
            
        has_permission = await check_permission(self.permission_name, db, user_id)
        print(f"has_permission: {has_permission}")
        if not has_permission:
            logger.warning(f"Permission denied: User {user_id} lacks {self.permission_name}")
            raise HTTPException(
                status_code=403, 
                detail=f"Permission denied: Missing required permission"
            )
            
        return auth_data

# Convenience function for checking ownership
async def is_card_owner(db: AsyncSession, card_id: int, user_id: int) -> bool:
    """
    Check if a user is the owner of a specific card.
    Returns True if user owns the card, False otherwise.
    """
    try:
        query = text("""
            SELECT 1 FROM card_data
            WHERE id = :card_id AND user_id = :user_id
        """)
        result = await db.execute(query, {"card_id": card_id, "user_id": user_id})
        is_owner = result.scalar_one_or_none() is not None
        
        if is_owner:
            logger.info(f"User {user_id} is owner of card {card_id}")
        else:
            logger.info(f"User {user_id} is NOT owner of card {card_id}")
            
        return is_owner
    except Exception as e:
        logger.error(f"Error checking card ownership for user {user_id}, card {card_id}: {e}")
        return False

# Helper for authorization logic
async def can_access_card(db: AsyncSession, card_id: int, auth_data: Dict[str, Any]) -> bool:
    """
    Check if the authenticated user can access a specific card.
    Returns True if access is allowed, False otherwise.
    
    Access rules:
    1. User is the card owner
    2. User has admin role
    3. User has view_any_user_cards permission
    """
    user_id = get_user_id(auth_data)
    
    # Admin can access any card
    if is_admin(auth_data):
        return True
        
    # Check if user is card owner
    is_owner = await is_card_owner(db, card_id, user_id)
    if is_owner:
        return True
        
    # Check for specific permission
    has_permission = await check_permission(Permissions.VIEW_ANY_USER_CARDS, db, user_id)
    return has_permission
