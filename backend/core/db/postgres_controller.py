"""
PostgreSQL Controller for Bitcoin quotes data.
"""
import psycopg2
import pandas as pd
import polars as pl
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


class PostgreSQLController:
    """Controller for PostgreSQL database operations."""
    
    def __init__(self, connection_params: Dict[str, any]):
        """
        Initialize the PostgreSQL controller.
        
        Args:
            connection_params: Dictionary containing database connection parameters:
                - database: Database name
                - user: Username
                - password: Password
                - host: Host address
                - port: Port number
        """
        self.connection_params = connection_params
        self._connection: Optional[psycopg2.extensions.connection] = None
        self._cursor: Optional[psycopg2.extensions.cursor] = None
        
    def connect(self) -> None:
        """Establish connection to the PostgreSQL database."""
        try:
            self._connection = psycopg2.connect(**self.connection_params)
            self._connection.autocommit = False
            self._cursor = self._connection.cursor()
            logger.info("Successfully connected to PostgreSQL database")
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to PostgreSQL database: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close the database connection."""
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Disconnected from PostgreSQL database")
    
    def close(self) -> None:
        """Alias for disconnect()."""
        self.disconnect()
    
    def _ensure_connected(self) -> None:
        """Ensure that a connection exists, connecting if necessary."""
        if self._connection is None or self._connection.closed:
            self.connect()
    
    def get_btc_quotes(self, start_date: str = "1900-01-01", end_date: str = "2025-12-31") -> pl.DataFrame:
        """
        Retrieve Bitcoin price quotes from the database.
        
        Args:
            start_date: Start date for the query (inclusive)
            end_date: End date for the query (inclusive)
            
        Returns:
            Polars DataFrame with columns: date_, close_
        """
        self._ensure_connected()
        
        query = """
            SELECT date_, close_ 
            FROM quotes 
            WHERE date_ >= %s AND date_ <= %s 
            ORDER BY date_
        """
        
        try:
            # Execute query using pandas for compatibility
            df_pandas = pd.read_sql(query, self._connection, params=(start_date, end_date))
            
            # Convert to Polars DataFrame
            df_polars = pl.from_pandas(df_pandas)
            
            # Ensure date column is properly typed
            df_polars = df_polars.with_columns(pl.col('date_').cast(pl.Date))
            
            logger.info(f"Retrieved {len(df_polars)} Bitcoin quotes from {start_date} to {end_date}")
            return df_polars
            
        except psycopg2.Error as e:
            logger.error(f"Failed to execute query: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while retrieving Bitcoin quotes: {e}")
            raise
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> pl.DataFrame:
        """
        Execute a custom SQL query and return results as Polars DataFrame.
        
        Args:
            query: SQL query string
            params: Optional tuple of query parameters
            
        Returns:
            Polars DataFrame with query results
        """
        self._ensure_connected()
        
        try:
            df_pandas = pd.read_sql(query, self._connection, params=params)
            df_polars = pl.from_pandas(df_pandas)
            logger.info(f"Executed query successfully, returned {len(df_polars)} rows")
            return df_polars
            
        except psycopg2.Error as e:
            logger.error(f"Failed to execute query: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while executing query: {e}")
            raise
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        return False
