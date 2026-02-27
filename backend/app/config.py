"""
Configuration module for the FastAPI application
"""
import os
from typing import Optional

class Settings:
    """Application settings loaded from environment variables"""
    
    def __init__(self):
        self.system_env: str = os.getenv("SYSTEM_ENV", "prod")
        self.debug: bool = os.getenv("DEBUG", "false").lower() == "true"
        self.port: int = int(os.getenv("PORT", "8002"))
        self.host: str = os.getenv("HOST", "0.0.0.0")
        
        # PostgreSQL Configuration
        self.postgres_db: str = os.getenv("POSTGRES_DB", "btc")
        self.postgres_user: str = os.getenv("POSTGRES_USER", "postgres")
        self.postgres_password: str = os.getenv("POSTGRES_PASSWORD", "")
        self.postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
        self.postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
        
        # Delta Table Paths
        self.inputs_address_view: str = os.getenv("INPUTS_ADDRESS_VIEW", "")
        self.outputs_address_view: str = os.getenv("OUTPUTS_ADDRESS_VIEW", "")
        self.pnl_stats: str = os.getenv("PNL_STATS", "")
        
    @property
    def is_dev(self) -> bool:
        return self.system_env.lower() == "dev"
    
    @property
    def is_prod(self) -> bool:
        return self.system_env.lower() == "prod"
    
    @property
    def postgres_connection_params(self) -> dict:
        """Return PostgreSQL connection parameters as a dictionary."""
        return {
            "database": self.postgres_db,
            "user": self.postgres_user,
            "password": self.postgres_password,
            "host": self.postgres_host,
            "port": self.postgres_port
        }
    
    @property
    def table_paths(self) -> dict:
        """Return Delta table paths as a dictionary."""
        return {
            "inputs": self.inputs_address_view,
            "outputs": self.outputs_address_view,
            "pnl_stats": self.pnl_stats
        }
    
    @property
    def postgres_db_url(self) -> str:
        """Return PostgreSQL database URL for SQLAlchemy + asyncpg."""
        return f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

# Global settings instance
settings = Settings()