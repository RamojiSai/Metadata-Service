"""Logging configuration and utilities"""

import logging
import os
from datetime import datetime
from functools import wraps
from pathlib import Path


# Configure logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "logs/app.log")

# Create logs directory if it doesn't exist
log_dir = Path(LOG_FILE).parent
log_dir.mkdir(parents=True, exist_ok=True)

# Create logger
logger = logging.getLogger("metadata_service")
logger.setLevel(getattr(logging, LOG_LEVEL))

# Create formatters
file_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
)
console_formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s'
)

# File handler
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(file_formatter)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(console_formatter)

# Add handlers
logger.addHandler(file_handler)
logger.addHandler(console_handler)


def log_error(error_type: str, message: str, details: dict = None):
    """Log error with structured format"""
    error_info = {
        "timestamp": datetime.now().isoformat(),
        "error_type": error_type,
        "message": message,
        "details": details or {}
    }
    logger.error(f"{error_type}: {message}", extra=error_info)


def log_info(message: str, details: dict = None):
    """Log info message"""
    logger.info(message, extra={"details": details or {}})


def log_warning(message: str, details: dict = None):
    """Log warning message"""
    logger.warning(message, extra={"details": details or {}})


def log_debug(message: str, details: dict = None):
    """Log debug message"""
    logger.debug(message, extra={"details": details or {}})


def log_exception(func):
    """Decorator to log exceptions"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            log_error(
                error_type=type(e).__name__,
                message=str(e),
                details={
                    "function": func.__name__,
                    "args": str(args),
                    "kwargs": str(kwargs)
                }
            )
            raise
    return wrapper


# Export logger instance
__all__ = ['logger', 'log_error', 'log_info', 'log_warning', 'log_debug', 'log_exception']