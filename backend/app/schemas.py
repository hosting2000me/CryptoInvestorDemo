"""
Pydantic schemas for the FastAPI application
"""
from pydantic import BaseModel
from typing import Optional, List


class BaseResponse(BaseModel):
    """Base response model"""
    success: bool = True
    message: Optional[str] = None


class HealthCheckResponse(BaseModel):
    """Health check response model"""
    status: str
    timestamp: Optional[str] = None


class User(BaseModel):
    """User model"""
    id: int
    name: str
    email: str


class UserCreateRequest(BaseModel):
    """Request model for creating a user"""
    name: str
    email: str


class UserListResponse(BaseModel):
    """Response model for user list"""
    users: List[User]
    total: int