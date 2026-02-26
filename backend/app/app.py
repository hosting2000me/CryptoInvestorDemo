import os
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse, FileResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
import asyncio
import json
import time
from typing import AsyncGenerator, Dict, Set

from config import settings
from schemas import HealthCheckResponse, UserListResponse, UserCreateRequest, User

# Определяем окружение для документации
# Документация доступна только в dev окружении для безопасности

if settings.is_dev:
    docs_url = "/docs"
    redoc_url = "/redoc"
    openapi_url = "/openapi.json"
else:  # prod или любое другое значение
    docs_url = None
    redoc_url = None
    openapi_url = None

app = FastAPI(
    title="Minimal FastAPI Application",
    description="A minimal FastAPI application with static files and endpoints",
    version="1.0.0",
    docs_url=docs_url,
    redoc_url=redoc_url,
    openapi_url=openapi_url,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"https://(.+\.)?yourdomain\.com",
    allow_methods=["*"],  # Allow all methods
    allow_headers=["*"],  # Allow all headers
)

# Mount static files directory
app.mount("/assets", StaticFiles(directory="./static/assets"), name="assets")

@app.get("/")
def read_root():
    return FileResponse("./static/index.html")

@app.get("/health", response_model=HealthCheckResponse)
def health_check():
    return HealthCheckResponse(status="healthy")

@app.get("/api/example")
def example_endpoint():
    return {"message": "This is an example endpoint"}

# Additional example endpoints
@app.get("/api/users", response_model=UserListResponse)
def get_users():
    return UserListResponse(users=[], total=0)

@app.post("/api/users", response_model=User)
def create_user(user_data: UserCreateRequest):
    # In a real application, this would create a user in the database
    new_user = User(id=1, name=user_data.name, email=user_data.email)
    return new_user

@app.get("/api/items/{item_id}")
def get_item(item_id: int, q: str = None):
    return {"item_id": item_id, "q": q}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        workers=1,
        loop="auto",
        timeout_keep_alive=180,  
        timeout_graceful_shutdown=5,
        log_level="debug",
        access_log=True
    )       