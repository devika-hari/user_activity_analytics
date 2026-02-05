from config import get_env
from logging_config import setup_logging
import logging
from sqlalchemy import create_engine,text
from sqlalchemy.exc import SQLAlchemyError
import json
import pandas as pd

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)


# DB config
DB_URI = get_env("DB_URI")
engine = create_engine(DB_URI)

def run_transform_and_load():
    """Moves data from staging.stg_logs to marts schema"""
    logger.info("Starting Transform and Load step (Staging -> Marts)")

    try:
        with engine.begin() as conn:
            # Update dim_users with any new user_ids
            logger.debug("Updating dim_users...")
            conn.execute(text("""
                INSERT INTO marts.dim_users (user_id)
                SELECT DISTINCT user_id 
                FROM staging.stg_logs 
                WHERE is_processed IS FALSE
                ON CONFLICT (user_id) DO NOTHING;
            """))

            # Update dim_actions with any new action_types
            logger.debug("Updating dim_actions...")
            conn.execute(text("""
                INSERT INTO marts.dim_actions (action_type)
                SELECT DISTINCT action_type 
                FROM staging.stg_logs 
                WHERE is_processed IS FALSE
                ON CONFLICT (action_type) DO NOTHING;
            """))

            # Load fact_user_actions by joining staging to the dimensions
            logger.debug("Loading fact_user_actions...")
            conn.execute(text("""
                INSERT INTO marts.fact_user_actions (user_key, action_key, event_timestamp, device, location)
                SELECT 
                    u.user_key, 
                    a.action_key, 
                    s.timestamp, 
                    s.device, 
                    s.location
                FROM staging.stg_logs s
                JOIN marts.dim_users u ON s.user_id = u.user_id
                JOIN marts.dim_actions a ON s.action_type = a.action_type
                WHERE s.is_processed IS FALSE;
            """))

            # Mark records as processed in staging
            logger.debug("Marking staging records as processed")
            conn.execute(text("""
                UPDATE staging.stg_logs 
                SET is_processed=TRUE
                WHERE is_processed IS FALSE ;
            """))

        logger.info("Transform and Load completed successfully.")

    except SQLAlchemyError as e:
        logger.error(f"Database transformation failed: {e}")
        # engine.begin() automatically rolls back on exception
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise

if __name__ == "__main__":
    run_transform_and_load()