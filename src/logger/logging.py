"""
Logging configuration for the NYC Taxi Trip Duration MLOps project.

This module provides thread-safe logging configuration with rotating file
handlers and console output. It prevents duplicate handlers and supports
module-level loggers.
"""

import logging
import os
import sys
import threading
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

# Constants for log configuration
LOG_DIR = 'logs'
LOG_FILE = 'nyc_taxi_mlops.log'
MAX_LOG_SIZE = 10 * 1024 * 1024  # 10 MB
BACKUP_COUNT = 5  # Number of backup log files to keep

# Thread-safe configuration
_config_lock = threading.Lock()
_configured = False


def get_log_file_path() -> Path:
    """
    Get the path to the log file, creating directories if necessary.

    Returns:
        Path to the log file
    """
    # Get project root (two levels up from this file)
    root_dir = Path(__file__).parent.parent.parent
    log_dir_path = root_dir / LOG_DIR

    # Create log directory if it doesn't exist
    log_dir_path.mkdir(parents=True, exist_ok=True)

    return log_dir_path / LOG_FILE


def configure_logging(
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    force_reconfigure: bool = False
) -> None:
    """
    Configure logging with rotating file handler and console handler.

    This function is thread-safe and idempotent - calling it multiple times
    will not create duplicate handlers unless force_reconfigure=True.

    Args:
        level: Logging level (default: INFO)
        log_file: Optional custom log file path
        force_reconfigure: If True, removes existing handlers and reconfigures
    """
    global _configured

    with _config_lock:
        # Skip if already configured and not forcing reconfiguration
        if _configured and not force_reconfigure:
            return

        # Get root logger
        root_logger = logging.getLogger()

        # Remove existing handlers if forcing reconfiguration
        if force_reconfigure:
            for handler in root_logger.handlers[:]:
                handler.close()
                root_logger.removeHandler(handler)

        # Set logging level
        root_logger.setLevel(logging.DEBUG)  # Capture all, filter at handler level

        # Define formatter
        formatter = logging.Formatter(
            "[ %(asctime)s ] %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # File handler with rotation
        log_file_path = Path(log_file) if log_file else get_log_file_path()
        log_file_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = RotatingFileHandler(
            log_file_path,
            maxBytes=MAX_LOG_SIZE,
            backupCount=BACKUP_COUNT,
            encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(level)

        # Add handlers to root logger if they don't already exist
        if not any(isinstance(h, RotatingFileHandler) for h in root_logger.handlers):
            root_logger.addHandler(file_handler)

        if not any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers):
            root_logger.addHandler(console_handler)

        _configured = True


def get_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    """
    Get a logger with the given name.

    Ensures logging is configured before returning the logger.

    Args:
        name: Name of the logger (typically __name__)
        level: Optional logging level for this specific logger

    Returns:
        Configured logger instance
    """
    # Ensure logging is configured
    if not _configured:
        configure_logging()

    logger = logging.getLogger(name)

    if level is not None:
        logger.setLevel(level)

    return logger


def reset_logging_config() -> None:
    """
    Reset the logging configuration.

    Useful for testing or when you need to completely reconfigure logging.
    """
    global _configured

    with _config_lock:
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            handler.close()
            root_logger.removeHandler(handler)
        _configured = False


# Configure logging on module import (thread-safe)
configure_logging()