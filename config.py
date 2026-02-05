# config.py
import os
from dotenv import load_dotenv
load_dotenv()

class ConfigError(RuntimeError):
    """Raised when required configuration is missing or invalid."""
    pass


REQUIRED_ENV_VARS = [
    "DATA_PATH",
    "PROCESSED_PATH",
    "ERROR_RECORDS",
    "ERROR_LOG_PATH",
    "DEBUG_LOG_PATH",
    "DB_URI"
]


def validate_env() -> None:
    """
    Validates that all required environment variables are present.
    Raises ConfigError if any are missing.
    """
    missing = []

    for var in REQUIRED_ENV_VARS:
        value = os.getenv(var)
        if value is None or value.strip() == "":
            missing.append(var)

    if missing:
        raise ConfigError(
            f"Missing required environment variables: {', '.join(missing)}"
        )


def get_env(name: str) -> str:
    """
    Safe accessor for environment variables.
    Assumes validate_env() has already been called.
    """
    return os.environ[name]

validate_env()
