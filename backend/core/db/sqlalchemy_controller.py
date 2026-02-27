"""
PostgreSQL Controller using SQLAlchemy + asyncpg for Bitcoin quotes data.
"""
import asyncpg
from sqlalchemy.ext.asyncio import create_async_engine, AsyncConnection
from sqlalchemy import text
from typing import Union, Dict, List, Any, Optional
from sqlalchemy.engine.result import Result
from sqlalchemy.ext.asyncio import AsyncEngine
import pandas as pd
import polars as pl
import logging

logger = logging.getLogger(__name__)


class SQLAlchemyController:
    """Controller for database operations using SQLAlchemy + asyncpg."""
    
    def __init__(self, db_url: str):
        """
        Initialize SQLAlchemy PostgreSQL controller.
        
        Args:
            db_url: Database connection URL (e.g., "postgresql+asyncpg://user:password@host:port/db")
        """
        self.db_url: str = db_url
        self.engine: AsyncEngine = create_async_engine(
            db_url,
            pool_pre_ping=True,     # Проверяет соединение перед использованием
            pool_recycle=3600,      # Пересоздает соединения каждый час
        )
    
    async def disconnect(self) -> None:
        """Close database connection."""
        if self.engine:
            await self.engine.dispose()
            logger.info("Disconnected from PostgreSQL database")
    
    async def get_btc_quotes(self, start_date: str = "1900-01-01", end_date: str = "2025-12-31") -> pl.DataFrame:
        """
        Retrieve Bitcoin price quotes from database.
        
        Args:
            start_date: Start date for the query (inclusive)
            end_date: End date for the query (inclusive)
            
        Returns:
            Polars DataFrame with columns: date_, close_
        """
        query = """
            SELECT date_, close_ 
            FROM quotes 
            WHERE date_ >= :start_date AND date_ <= :end_date 
            ORDER BY date_
        """
        
        try:
            async with self.engine.connect() as conn:
                result = await conn.execute(text(query), {
                    "start_date": start_date,
                    "end_date": end_date
                })
                
                # Convert SQLAlchemy result to list of dictionaries
                rows = result.mappings().fetchall()
                
                # Convert to pandas DataFrame
                df_pandas = pd.DataFrame(rows)
                
                # Convert to Polars DataFrame
                df_polars = pl.from_pandas(df_pandas)
                
                # Ensure date column is properly typed
                df_polars = df_polars.with_columns(pl.col('date_').cast(pl.Date))
                
                logger.info(f"Retrieved {len(df_polars)} Bitcoin quotes from {start_date} to {end_date}")
                return df_polars
                
        except Exception as e:
            logger.error(f"Failed to retrieve Bitcoin quotes: {e}")
            raise
    
    async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> pl.DataFrame:
        """
        Execute a custom SQL query and return results as Polars DataFrame.
        
        Args:
            query: SQL query string
            params: Optional dictionary of query parameters
            
        Returns:
            Polars DataFrame with query results
        """
        try:
            async with self.engine.connect() as conn:
                result = await conn.execute(text(query), params or {})
                rows = result.mappings().fetchall()
                df_pandas = pd.DataFrame(rows)
                df_polars = pl.from_pandas(df_pandas)
                logger.info(f"Executed query successfully, returned {len(df_polars)} rows")
                return df_polars
                
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise
    
    async def __aenter__(self):
        """Async context manager entry."""
        # Connection is established lazily when needed
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
        return False


# Helper function to create database URL from connection parameters
def create_db_url(params: Dict[str, Any]) -> str:
    """
    Create database URL from connection parameters.
    
    Args:
        params: Dictionary containing database connection parameters:
            - database: Database name
            - user: Username
            - password: Password
            - host: Host address
            - port: Port number
            
    Returns:
        Database URL string for asyncpg
    """
    return f"postgresql+asyncpg://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['database']}"
