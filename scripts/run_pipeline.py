import logging
from pathlib import Path

from extract_api_data import extract_data
from transform_crypto_data import transform_data
from load_to_postgres import load_to_postgres


BASE_DIR = Path(__file__).resolve().parent.parent
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

log_file = LOGS_DIR / "pipeline.log"
# Set up logging
logger = logging.getLogger("pipeline_logger")
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


def run_pipeline():
    try:
        logger.info("Starting ETL pipeline...")

        logger.info("Running extraction step...")
        raw_file = extract_data()
        logger.info(f"Extraction completed. Raw file: {raw_file}")

        logger.info("Running transformation step...")
        processed_file = transform_data()
        logger.info(f"Transformation completed. Processed file: {processed_file}")

        logger.info("Running load step...")
        load_to_postgres()
        logger.info("Load step completed successfully.")

        logger.info("ETL pipeline completed successfully!")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    run_pipeline()