import logging
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv


load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DATA_DIR = BASE_DIR / "data" / "processed"
LOGS_DIR = BASE_DIR / "logs"

LOGS_DIR.mkdir(parents=True, exist_ok=True)

log_file = LOGS_DIR / "load.log"
# Set up logging
logger = logging.getLogger("load_logger")
logger.setLevel(logging.INFO)

if logger.hasHandlers():
    logger.handlers.clear()

file_handler = logging.FileHandler(log_file)
stream_handler = logging.StreamHandler()

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


def get_latest_processed_file():
    csv_files = list(PROCESSED_DATA_DIR.glob("*.csv"))

    if not csv_files:
        logger.error("No processed CSV files found!")
        raise FileNotFoundError("No processed CSV files found!")

    latest_file = max(csv_files, key=lambda file: file.stat().st_mtime)
    logger.info(f"Latest processed file selected: {latest_file}")
    return latest_file


def load_to_postgres():
    try:
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")

        connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)

        logger.info("Connected to PostgreSQL successfully")

        file_path = get_latest_processed_file()
        df = pd.read_csv(file_path)

        logger.info(f"Loaded DataFrame with shape: {df.shape}")

        if df.empty:
            logger.error("DataFrame is empty, nothing to load")
            raise ValueError("No data to load")

        table_name = "crypto_market_data"

        df.to_sql(
            table_name,
            engine,
            if_exists="append",
            index=False
        )

        logger.info(f"Data successfully loaded into table: {table_name}")
        logger.info(f"Rows inserted: {len(df)}")

    except Exception as e:
        logger.error(f"Error during loading: {e}")
        raise


if __name__ == "__main__":
    load_to_postgres()