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
        
    @property
    def is_dev(self) -> bool:
        return self.system_env.lower() == "dev"
    
    @property
    def is_prod(self) -> bool:
        return self.system_env.lower() == "prod"

# Global settings instance
settings = Settings()