# Crypto Analytics Module

This module provides functionality for analyzing Bitcoin addresses and calculating financial metrics. It was extracted from Jupyter Notebook (`plot.py`) and refactored into independent, testable components.

## Architecture

The module consists of three main components:

### 1. Database Controllers (`core/db/`)

#### PostgreSQL Controller (psycopg2)
Handles all PostgreSQL database operations for Bitcoin price quotes using psycopg2.

**Key Features:**
- Connection management with context manager support
- Query Bitcoin price quotes from `quotes` table
- Return data in Polars DataFrame format
- Error handling and logging

**Usage:**
```python
from core.db.postgres_controller import PostgreSQLController

# Initialize with connection parameters
controller = PostgreSQLController({
    "database": "btc",
    "user": "postgres",
    "password": "password",
    "host": "localhost",
    "port": 5432
})

# Get Bitcoin quotes
btc_quotes = controller.get_btc_quotes("2020-01-01", "2024-12-31")

# Use as context manager
with controller as ctrl:
    quotes = ctrl.get_btc_quotes()
```

#### PostgreSQL Controller (SQLAlchemy + asyncpg)
Alternative PostgreSQL controller using SQLAlchemy with asyncpg driver for async operations.

**Key Features:**
- Async/await support for better performance
- Connection pooling with automatic recycling
- Same API as psycopg2 controller
- Returns data in Polars DataFrame format

**Usage:**
```python
from core.db.postgres_controller import SQLAlchemyPostgreSQLController
from app.config import settings

# Initialize with database URL
controller = SQLAlchemyPostgreSQLController(settings.postgres_db_url)

# Get Bitcoin quotes (async)
btc_quotes = await controller.get_btc_quotes("2020-01-01", "2024-12-31")

# Use as async context manager
async with controller as ctrl:
    quotes = await ctrl.get_btc_quotes()
```

**Note:** Both controllers provide the same functionality. Use SQLAlchemy version for async operations and better connection pooling.

#### Delta Table Controller
Handles all Delta table operations for Bitcoin address transactions.

**Key Features:**
- Read input/output transactions from Delta tables
- Partition-based queries using CRC32 hashing
- Retrieve PNL statistics
- Error handling and logging

**Usage:**
```python
from core.db.delta_controller import DeltaTableController

# Initialize with table paths
controller = DeltaTableController({
    "inputs": "/data/btc/views/inputs_address",
    "outputs": "/data/btc/views/outputs_address",
    "pnl_stats": "/data/btc/pnl_stats"
})

# Get transactions for an address
input_txs = controller.get_input_transactions("bc1qaddress123")
output_txs = controller.get_output_transactions("bc1qaddress123")
pnl_stats = controller.get_pnl_stats("2023-10-01")
```

### 2. Analytics Service (`core/services/`)

#### CryptoAnalytics
Orchestrates data retrieval and performs financial calculations.

**Key Features:**
- Calculate profit/loss for Bitcoin addresses
- Compute Sharpe ratio and drawdown
- Calculate exposure and days in market
- Generate benchmark comparisons
- Filter top addresses by profit

**Usage:**
```python
from core.services.crypto_analytics import CryptoAnalytics
from core.db.postgres_controller import PostgreSQLController
from core.db.delta_controller import DeltaTableController

# Initialize controllers
postgres_ctrl = PostgreSQLController(connection_params)
delta_ctrl = DeltaTableController(table_paths)

# Initialize analytics service
analytics = CryptoAnalytics(postgres_ctrl, delta_ctrl)

# Get address statistics
stats = analytics.get_address_transactions("bc1qaddress123")
print(f"Profit: {stats['profit_pct']:.2%}")
print(f"Sharpe Ratio: {stats['sharpe_ratio']:.2f}")

# Get balance history
balance = analytics.get_address_balance("bc1qaddress123")

# Get benchmark metrics
benchmark = analytics.get_benchmark_metrics()

# Get top addresses by profit
top_addresses = analytics.get_top_addresses_by_profit("2023-10-01", {
    "profit2btc_min": 50000.0,
    "count_out_min": 5
})
```

### 3. Data Models (`core/models/`)

#### Pydantic Schemas
Define data models for type safety and validation.

**Available Schemas:**
- `BitcoinQuote`: Bitcoin price quote data
- `Transaction`: Bitcoin transaction data
- `AddressStats`: Bitcoin address statistics
- `BenchmarkMetrics`: Benchmark metrics
- `AddressFilter`: Filter criteria for addresses

## Configuration

All configuration is managed through environment variables in `.env` file:

```env
# PostgreSQL Configuration
POSTGRES_DB=btc
POSTGRES_USER=postgres
POSTGRES_PASSWORD=dbadmin1975
POSTGRES_HOST=postgres17
POSTGRES_PORT=5432

# Delta Table Paths
INPUTS_ADDRESS_VIEW=/data/btc/views/inputs_address
OUTPUTS_ADDRESS_VIEW=/data/btc/views/outputs_address
PNL_STATS=/data/btc/pnl_stats
```

## API Endpoints

The FastAPI application provides the following endpoints:

### Address Statistics
- `GET /api/crypto/address/{address}/stats` - Get comprehensive statistics for a Bitcoin address
- `GET /api/crypto/address/{address}/balance` - Get balance history for a Bitcoin address
- `GET /api/crypto/address/{address}/full` - Get full data including statistics and balance history

### Benchmark
- `GET /api/crypto/benchmark` - Get benchmark metrics for Bitcoin (buy and hold strategy)

### Top Addresses
- `GET /api/crypto/top-addresses` - Get top addresses by profit for a specific date

**Query Parameters for `/api/crypto/top-addresses`:**
- `date` (required): Date in format YYYY-MM-DD
- `profit2btc_min`: Minimum profit in BTC
- `max_btc_min`: Minimum maximum BTC held
- `btcvalue_ratio_min`: Minimum current BTC value ratio to max (0-1)
- `count_out_min`: Minimum number of output transactions
- `first_in_after`: First input after this date (YYYY-MM-DD)

## Testing

Run unit tests:

```bash
# Run all core tests
pytest backend/tests/core/

# Run specific test
pytest backend/tests/core/test_postgres_controller.py

# Run with coverage
pytest backend/tests/core/ --cov=backend/core --cov-report=html
```

The core modules (`core/`) are independent of FastAPI and can be tested separately without starting the application.

## Dependencies

- `psycopg2-binary` - PostgreSQL database adapter
- `polars` - Data manipulation library
- `pandas` - Data analysis library
- `deltalake` - Delta table operations
- `pyarrow` - Apache Arrow Python library
- `empyrical` - Financial metrics calculations
- `pyfolio` - Portfolio analytics
- `numpy` - Numerical computing

### Additional Dependencies for SQLAlchemy + asyncpg (alternative)
- `sqlalchemy` - SQL toolkit and ORM
- `asyncpg` - PostgreSQL async driver

## Design Principles

1. **Independence**: All modules are independent of FastAPI and can be tested separately
2. **Type Safety**: Use type hints throughout for better IDE support
3. **Error Handling**: All controllers handle errors gracefully
4. **Resource Management**: Use context managers for database connections
5. **Logging**: Comprehensive logging for debugging and monitoring
6. **Testing**: Unit tests for all components with mocked dependencies

## File Structure

```
backend/core/
├── __init__.py
├── db/
│   ├── __init__.py
│   ├── postgres_controller.py    # PostgreSQL database operations (psycopg2)
│   ├── postgres_sqlalchemy_controller.py  # PostgreSQL database operations (SQLAlchemy + asyncpg)
│   └── delta_controller.py      # Delta table operations
├── services/
│   ├── __init__.py
│   └── crypto_analytics.py     # Financial calculations
└── models/
    ├── __init__.py
    └── schemas.py               # Pydantic data models
```

## Example Response

```json
{
  "address": "bc1qaddress123",
  "profit_pct": 0.45,
  "sharpe_ratio": 1.23,
  "drawdown": -0.15,
  "exposure": 0.75,
  "count_days_in_market": 365,
  "benchmark_profit": 0.30,
  "benchmark_sharpe": 0.95,
  "benchmark_drawdown": -0.20
}
```

## Notes

- All calculations use satoshis for Bitcoin values (1 BTC = 100,000,000 satoshis)
- Sharpe ratio is calculated with a risk-free rate of 0
- Drawdown is calculated as a negative percentage
- Exposure represents the average ratio of BTC held to maximum BTC held
- Days in market counts days with BTC holdings greater than 0.001 BTC
- Two PostgreSQL controllers are available: psycopg2 (synchronous) and SQLAlchemy + asyncpg (asynchronous)
