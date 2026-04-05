import os
import logging
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# paths
BASE_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DATA_DIR = BASE_DIR / "data" /"processed"
LOGS_DIR = BASE_DIR / "logs"

LOGS_DIR.mkdir(parents=True, exist_ok=True)

log_file = LOGS_DIR / "load.log"

logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ] 
)

def get_latest_processed_file():
    csv_files = list(PROCESSED_DATA_DIR.glob("*.csv"))

    if not csv_files:
        logging.error("No csv files found in processed data directory")
        raise FileNotFoundError("No csv files found in processed data directory")
    
    latest_file = max(csv_files, key=lambda file: file.stat().st_mtime)
    logging.info(f"Latest processed file seleccted : {latest_file}")

    return latest_file


def load_to_postgres():
    try:
        #Database connection details
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")

        connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

        engine = create_engine(connection_string)

        logging.info("Database connection established successfully")

        # Get the latest processed file
        file_path = get_latest_processed_file()

        df = pd.read_csv(file_path)

        logging.info(f"Loaded dataframe with shape {df.shape}")

        if df.empty:
            logging.error("Dataframe is empty")
            raise ValueError("No data to load")

        # Load data to database
        table_name = "crypto_db"

        df.to_sql(
            table_name,
            engine,
            if_exists = "append",
            index=False
        )

        logging.info(f"Data successfuly loaded into {table_name}")
        logging.info(f"Rows inserted : {len(df)}")

    except Exception as e:
        logging.error(f"Error during loading {e}")
        raise




if __name__ == "__main__":
    load_to_postgres()