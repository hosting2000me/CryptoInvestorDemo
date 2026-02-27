"""
Database controllers for PostgreSQL and Delta Tables.
"""
from .delta_controller import DeltaTableController

# Conditionally import PostgreSQL controllers if psycopg2 is available
try:
    from .postgres_controller import PostgreSQLController
    from .sqlalchemy_controller import SQLAlchemyController
    _has_postgres = True
except ImportError:
    _has_postgres = False
    PostgreSQLController = None
    SQLAlchemyController = None

__all__ = [
    "PostgreSQLController",
    "DeltaTableController",
    "SQLAlchemyController",
]
