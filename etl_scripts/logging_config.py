# logging_config.py
import logging
from logging.handlers import RotatingFileHandler
from etl_scripts.config import get_env, validate_env
import os

def setup_logging():
    # Get paths from config
    error_log_path = get_env("ERROR_LOG_PATH")
    debug_log_path = get_env("DEBUG_LOG_PATH")

    # Ensure directories exist
    os.makedirs(os.path.dirname(error_log_path), exist_ok=True)
    os.makedirs(os.path.dirname(debug_log_path), exist_ok=True)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Remove old handlers to avoid duplicates if setup_logging is called multiple times
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.DEBUG)

    # Error log
    error_handler = RotatingFileHandler(
        error_log_path, maxBytes=5_000_000, backupCount=3
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    root.addHandler(error_handler)

    # Debug log
    debug_handler = RotatingFileHandler(
        debug_log_path, maxBytes=5_000_000, backupCount=3
    )
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(formatter)
    root.addHandler(debug_handler)

