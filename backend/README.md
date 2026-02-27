# Crypto Investor Analytics Backend

A FastAPI application for analyzing Bitcoin addresses with financial metrics including profit/loss, Sharpe ratio, and drawdown calculations. The application integrates PostgreSQL for Bitcoin price quotes and Delta tables for transaction data.

## Project Structure

```
backend/
├── core/                          # Crypto analytics modules (independent from FastAPI)
│   ├── db/
│   │   ├── postgres_controller.py   # PostgreSQL database operations
│   │   └── delta_controller.py     # Delta table operations
│   ├── services/
│   │   └── crypto_analytics.py    # Financial calculations
│   └── models/
│       └── schemas.py              # Pydantic data models
├── app/                           # FastAPI application
│   ├── app.py                     # Main application with endpoints
│   ├── config.py                  # Configuration management
│   ├── logger.py                  # Logging setup
│   └── schemas.py                # API schemas
├── tests/
│   ├── core/                      # Unit tests for core modules
│   │   ├── test_postgres_controller.py
│   │   ├── test_delta_controller.py
│   │   └── test_crypto_analytics.py
│   └── fixtures/                  # Test data
│       └── sample_data.py
├── static/                        # Static files for SPA
├── .env                          # Environment variables
├── requirements.txt                # Python dependencies
└── Dockerfile                     # Docker configuration
```

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

### Crypto Analytics
- **PostgreSQL Integration**: Query Bitcoin price quotes from database (supports psycopg2 and SQLAlchemy + asyncpg)
- **Delta Table Access**: Read transaction data from Delta tables with partition-based queries
- **Financial Metrics**: Calculate profit/loss, Sharpe ratio, drawdown, and exposure
- **Benchmark Comparison**: Compare address performance against buy-and-hold strategy
- **Top Addresses**: Filter and rank addresses by various criteria

### Application
- FastAPI application with RESTful endpoints
- Static file serving for SPA
- Configuration management
- Logging setup
- Pydantic schema validation
- CORS middleware
- Comprehensive testing setup

### Architecture
- **Independent Modules**: Core functionality is independent of FastAPI and can be tested separately
- **Type Safety**: Full type hints throughout the codebase
- **Error Handling**: Graceful error handling with proper HTTP status codes
- **Resource Management**: Context managers for database connections
- **Alternative Implementations**: Two PostgreSQL controllers available (psycopg2 and SQLAlchemy + asyncpg)

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

### Crypto Analytics Endpoints

- `GET /api/crypto/address/{address}/stats` - Get comprehensive statistics for a Bitcoin address
  - Returns profit/loss, Sharpe ratio, drawdown, exposure, and benchmark metrics
- `GET /api/crypto/address/{address}/balance` - Get balance history for a Bitcoin address
  - Returns DataFrame with date and value columns
- `GET /api/crypto/address/{address}/full` - Get full data including statistics and balance history
- `GET /api/crypto/benchmark` - Get benchmark metrics for Bitcoin (buy and hold strategy)
- `GET /api/crypto/top-addresses` - Get top addresses by profit for a specific date
  - Query parameters: `date` (required), `profit2btc_min`, `max_btc_min`, `btcvalue_ratio_min`, `count_out_min`, `first_in_after`

### General Endpoints

- `GET /` - Root endpoint (serves static index.html)
- `GET /health` - Health check
- `GET /api/example` - Example API endpoint
- `GET /api/users` - Get users
- `POST /api/users` - Create user
- `GET /api/items/{id}` - Get item by ID

### Documentation

- `/docs` - API documentation (only in dev environment)
- `/redoc` - Alternative API documentation (only in dev environment)

For detailed information about the crypto analytics module, see [`core/README.md`](core/README.md).

## Environment Variables

### Application Settings
- `SYSTEM_ENV`: Environment setting ("dev" or "prod") - defaults to "prod"
- `DEBUG`: Enable debug mode - defaults to "false"
- `PORT`: Port to run the application on - defaults to "8002"
- `HOST`: Host to bind to - defaults to "0.0.0.0"

### PostgreSQL Configuration
- `POSTGRES_DB`: Database name - defaults to "btc"
- `POSTGRES_USER`: Database user - defaults to "postgres"
- `POSTGRES_PASSWORD`: Database password
- `POSTGRES_HOST`: Database host - defaults to "localhost"
- `POSTGRES_PORT`: Database port - defaults to "5432"

### Delta Table Paths
- `INPUTS_ADDRESS_VIEW`: Path to inputs address Delta table
- `OUTPUTS_ADDRESS_VIEW`: Path to outputs address Delta table
- `PNL_STATS`: Path to PNL statistics Delta table

See [`.env`](.env) for example configuration.

## Dependencies

### Core Dependencies
- `psycopg2-binary` - PostgreSQL database adapter (synchronous)
- `sqlalchemy` - SQL toolkit and ORM (alternative for async operations)
- `asyncpg` - PostgreSQL async driver (alternative for async operations)
- `polars` - Data manipulation library
- `pandas` - Data analysis library
- `deltalake` - Delta table operations
- `pyarrow` - Apache Arrow Python library
- `empyrical-reloaded` - Financial metrics calculations
- `pyfolio-reloaded` - Portfolio analytics
- `numpy` - Numerical computing

### Application Dependencies
- `fastapi` - Web framework
- `uvicorn` - ASGI server
- `pydantic` - Data validation
- `aiofiles` - Async file operations

### Testing Dependencies
- `pytest` - Testing framework
- `pytest-cov` - Coverage reporting
- `pytest-asyncio` - Async test support

## Testing

For detailed testing instructions, see [`TESTING.md`](TESTING.md).

Quick start:
```bash
# Install dependencies
pip install -r requirements.txt

# Run all tests
pytest tests/

# Run only core module tests
pytest tests/core/

# Run with coverage
pytest tests/ --cov=backend --cov-report=html
```

The core modules (`core/`) are independent of FastAPI and can be tested separately without starting the application.