# Testing Guide

This guide explains how to run the tests for the Crypto Analytics backend.

## Prerequisites

Before running tests, you need to install the dependencies:

```bash
# Navigate to the backend directory
cd backend

# Install dependencies using pip
pip install -r requirements.txt

# Or if you're using uv (as suggested by pyproject.toml)
uv pip install -r requirements.txt
```

## Running Tests

### Run All Tests

```bash
# From the backend directory
pytest tests/
```

### Run Core Module Tests Only

```bash
# Run only the crypto analytics module tests
pytest tests/core/
```

### Run Specific Test File

```bash
# Run PostgreSQL controller tests
pytest tests/core/test_postgres_controller.py

# Run Delta table controller tests
pytest tests/core/test_delta_controller.py

# Run crypto analytics service tests
pytest tests/core/test_crypto_analytics.py

# Run existing app tests
pytest tests/test_app.py
```

### Run with Verbose Output

```bash
# Show detailed test output
pytest tests/core/ -v
```

### Run with Coverage Report

```bash
# Generate HTML coverage report
pytest tests/core/ --cov=backend/core --cov-report=html

# Generate terminal coverage report
pytest tests/core/ --cov=backend/core --cov-report=term-missing
```

### Run Specific Test

```bash
# Run a specific test function
pytest tests/core/test_postgres_controller.py::TestPostgreSQLController::test_init

# Run tests matching a pattern
pytest tests/core/ -k "test_connect"
```

## Test Structure

```
backend/tests/
├── core/                           # Tests for crypto analytics modules
│   ├── test_postgres_controller.py   # PostgreSQL controller tests
│   ├── test_delta_controller.py     # Delta table controller tests
│   └── test_crypto_analytics.py    # Analytics service tests
├── fixtures/                       # Test data and fixtures
│   └── sample_data.py              # Sample data for testing
└── test_app.py                     # Existing FastAPI app tests
```

## Test Independence

The core module tests (`tests/core/`) are completely independent of FastAPI and can run without starting the application. They use mocked dependencies to test the business logic in isolation.

## Common Issues

### Module Not Found Errors

If you get `ModuleNotFoundError`, make sure you're running pytest from the `backend` directory:

```bash
cd backend
pytest tests/
```

### Import Errors

If you get import errors, ensure all dependencies are installed:

```bash
pip install -r requirements.txt
```

### Pytest Not Found

If pytest is not installed, install it:

```bash
pip install pytest pytest-cov
```

## Continuous Integration

To run tests as part of CI/CD pipeline:

```bash
# Run all tests with coverage
pytest tests/ --cov=backend --cov-report=xml --cov-report=term-missing
```

## Writing New Tests

When adding new tests:

1. Create test files in `tests/core/` following the naming convention `test_*.py`
2. Use the fixtures from `tests/fixtures/sample_data.py` for sample data
3. Mock external dependencies (PostgreSQL, Delta tables) using `unittest.mock`
4. Follow the existing test structure with descriptive test names
5. Add docstrings explaining what each test verifies

Example test structure:

```python
import pytest
from unittest.mock import Mock, patch

from core.services.crypto_analytics import CryptoAnalytics

class TestCryptoAnalytics:
    """Test suite for CryptoAnalytics."""
    
    def test_init(self):
        """Test service initialization."""
        # Arrange
        mock_postgres = Mock()
        mock_delta = Mock()
        
        # Act
        service = CryptoAnalytics(mock_postgres, mock_delta)
        
        # Assert
        assert service.postgres_controller == mock_postgres
        assert service.delta_controller == mock_delta
```
