import os
import shutil
from config import get_env
from logging_config import setup_logging
import logging
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import json
import pandas as pd

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

# Load paths & DB config
DATA_PATH = get_env("DATA_PATH")
PROCESSED_PATH = get_env("PROCESSED_PATH")
ERROR_RECORDS=get_env("ERROR_RECORDS")
DB_URI = get_env("DB_URI")

# Create SQLAlchemy engine-connection to db
engine = create_engine(DB_URI)

def parse_timestamp(ts: str):
    """Convert timestamp to ISO format"""
    try:
        return pd.to_datetime(ts).isoformat()
    except Exception as e:
        logger.error(f"Invalid timestamp: {ts}, error: {e}")
        return None

def clean_records(raw_data: list) -> pd.DataFrame:
    """Clean raw data and return DataFrame ready for staging table """
    cleaned = []
    bad_records = []

    for record in raw_data:
        # Skip missing mandatory fields
        if not record.get("user_id") or not record.get("action_type"):
            record["error_reason"] = "Missing user_id or action_type"
            logger.debug(f"Skipping record: {record}  due to missing user_id or action_type")
            bad_records.append(record)
            continue

        # Convert timestamp
        iso_ts = parse_timestamp(record.get("timestamp", ""))
        if not iso_ts:
            record["timestamp"]=None
            logger.debug(f"Timestamp error in record: {record}.")
            continue
        record["timestamp"] = iso_ts

        # Extract device & location
        metadata = record.get("metadata", {})
        record["device"] = metadata.get("device")
        record["location"] = metadata.get("location")
        record.pop("metadata", None)
        # New Column

        record["load_ts"] = datetime. now()
        cleaned.append(record)

    # Write bad records to JSON
    if bad_records:
        with open(ERROR_RECORDS, "w") as f:
            json.dump(bad_records, f, indent=2)
        logger.debug(f"Wrote {len(bad_records)} bad records to {ERROR_RECORDS}")

    df = pd.DataFrame(cleaned)
    logger.debug(f"Cleaned {len(df)} valid records out of {len(raw_data)}")
    return df

def insert_to_staging(df: pd.DataFrame):
    """Insert cleaned records into staging table"""
    if df.empty:
        logger.debug("No records to insert into staging table")
        return

    try:
        with engine.connect() as conn:
            df.to_sql("stg_logs", conn,schema="staging", if_exists="append", index=False)
        logger.debug(f"Inserted {len(df)} records into staging_user_actions")
        #move to processed after successful insertion
        move_to_processed()
    except SQLAlchemyError as e:
        logger.error(f"Failed to insert records into staging table: {e}")
        raise

def move_to_processed():
    """Move data file to Processed folder"""
    if not os.path.exists(PROCESSED_PATH):
        os.makedirs(PROCESSED_PATH)
    # Add a timestamp to filename
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_name = f"{timestamp_str}_raw_logs"
    dest = os.path.join(PROCESSED_PATH, dest_name)

    shutil.move(DATA_PATH, dest)
    logger.info(f"File moved to {dest}")


def run_extract():
    """Main extract function"""
    logger.debug("Starting extract step")

    # File Existence Check
    if not os.path.exists(DATA_PATH):
        logger.debug("No source file found. Exiting.")
        return
    try:
        with open(DATA_PATH, "r") as f:
            raw_data = json.load(f)
        if not raw_data:
            logger.warning("File is empty.")
            return

        logger.debug(f"Raw data count: {len(raw_data)} records")
        cleaned_df = clean_records(raw_data)
        insert_to_staging(cleaned_df)

    except SQLAlchemyError as e:
        logger.error(f"Database error. File remains in source for retry. Error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


if __name__ == "__main__":
    run_extract()
