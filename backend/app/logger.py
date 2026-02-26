"""
Logging configuration for the FastAPI application
"""
import logging
from datetime import datetime

# Create a custom logger
logger = logging.getLogger("minimal_fastapi_app")
logger.setLevel(logging.DEBUG)

# Create handlers
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create formatters and add it to handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(console_handler)

# Prevent propagation to root logger
logger.propagate = False

def log_info(message: str):
    """Log an info message with timestamp"""
    logger.info(f"[INFO] {datetime.now().isoformat()}: {message}")

def log_error(message: str):
    """Log an error message with timestamp"""
    logger.error(f"[ERROR] {datetime.now().isoformat()}: {message}")

def log_debug(message: str):
    """Log a debug message with timestamp"""
    logger.debug(f"[DEBUG] {datetime.now().isoformat()}: {message}")

def log_warning(message: str):
    """Log a warning message with timestamp"""
    logger.warning(f"[WARNING] {datetime.now().isoformat()}: {message}")