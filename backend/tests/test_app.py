"""
Basic tests for the FastAPI application
"""
import pytest
from fastapi.testclient import TestClient

# Import the app
from minimal_fastapi_skeleton.app.app import app

client = TestClient(app)

def test_read_root():
    """Test the root endpoint"""
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welcome to the Minimal FastAPI Application"}

def test_health_check():
    """Test the health check endpoint"""
    response = client.get("/health")
    assert response.status_code == 200
    assert "status" in response.json()
    assert response.json()["status"] == "healthy"

def test_example_endpoint():
    """Test the example API endpoint"""
    response = client.get("/api/example")
    assert response.status_code == 200
    assert "message" in response.json()

def test_get_users():
    """Test the get users endpoint"""
    response = client.get("/api/users")
    assert response.status_code == 200
    assert "users" in response.json()
    assert "total" in response.json()

def test_create_user():
    """Test the create user endpoint"""
    user_data = {
        "name": "John Doe",
        "email": "john@example.com"
    }
    response = client.post("/api/users", json=user_data)
    assert response.status_code == 200
    assert "id" in response.json()
    assert response.json()["name"] == user_data["name"]
    assert response.json()["email"] == user_data["email"]

def test_get_item():
    """Test the get item endpoint"""
    response = client.get("/api/items/1")
    assert response.status_code == 200
    assert "item_id" in response.json()
    assert response.json()["item_id"] == 1