# Minimal FastAPI Application

A minimal FastAPI application skeleton with static files and basic endpoints.

## Project Structure

```
minimal_fastapi_skeleton/
├── __init__.py
├── .env
├── Dockerfile
├── requirements.txt
├── app/
│   ├── __init__.py
│   ├── app.py
│   ├── config.py
│   ├── logger.py
│   └── schemas.py
├── static/
│   └── index.html
├── tests/
│   ├── __init__.py
│   └── test_app.py
└── logs/
```

## Features

- FastAPI application with basic endpoints
- Static file serving for SPA
- Configuration management
- Logging setup
- Pydantic schema validation
- CORS middleware
- Basic testing setup

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the application:
```bash
uvicorn minimal_fastapi_skeleton.app.app:app --reload --host 0.0.0.0 --port 8002
```

3. Or run with Docker:
```bash
docker build -t minimal-fastapi-app .
docker run -p 8002:8002 minimal-fastapi-app
```

## Endpoints

- `GET /` - Root endpoint
- `GET /health` - Health check
- `GET /api/example` - Example API endpoint
- `GET /api/users` - Get users
- `POST /api/users` - Create user
- `GET /api/items/{id}` - Get item by ID
- `/docs` - API documentation (only in dev environment)
- `/redoc` - Alternative API documentation (only in dev environment)

## Environment Variables

- `SYSTEM_ENV`: Environment setting ("dev" or "prod") - defaults to "prod"
- `DEBUG`: Enable debug mode - defaults to "false"
- `PORT`: Port to run the application on - defaults to "8002"
- `HOST`: Host to bind to - defaults to "0.0.0.0"

## Testing

Run tests with pytest:
```bash
pytest tests/