import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DATA_DIR = BASE_DIR / "data" / "processed"
LOGS_DIR = BASE_DIR / "logs"

PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

log_file = LOGS_DIR / "transform.log"
# Set up logging
logger = logging.getLogger("transform_logger")
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


def get_latest_raw_file():
    json_files = list(RAW_DATA_DIR.glob("*.json"))

    if not json_files:
        logger.error("No JSON files found in data/raw/")
        raise FileNotFoundError("No JSON files found in data/raw/")

    latest_file = max(json_files, key=lambda file: file.stat().st_mtime)
    logger.info(f"Latest raw file selected: {latest_file}")
    return latest_file


def categorize_market_cap(market_cap):
    if pd.isna(market_cap):
        return "unknown"
    elif market_cap >= 10_000_000_000:
        return "large"
    elif market_cap >= 1_000_000_000:
        return "medium"
    else:
        return "small"


def transform_data():
    try:
        raw_file_path = get_latest_raw_file()

        with open(raw_file_path, "r", encoding="utf-8") as file:
            raw_data = json.load(file)

        df = pd.DataFrame(raw_data)
        logger.info(f"Raw DataFrame created with shape: {df.shape}")

        selected_columns = [
            "id", "symbol", "name", "current_price", "market_cap",
            "market_cap_rank", "total_volume", "high_24h", "low_24h",
            "price_change_24h", "price_change_percentage_24h",
            "circulating_supply", "total_supply", "ath", "atl", "last_updated"
        ]

        df = df[selected_columns]
        df["last_updated"] = pd.to_datetime(df["last_updated"], errors="coerce")

        numeric_columns = [
            "current_price", "market_cap", "market_cap_rank", "total_volume",
            "high_24h", "low_24h", "price_change_24h",
            "price_change_percentage_24h", "circulating_supply",
            "total_supply", "ath", "atl"
        ]

        for column in numeric_columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

        df["price_range_24h"] = df["high_24h"] - df["low_24h"]
        df["market_cap_category"] = df["market_cap"].apply(categorize_market_cap)
        df["snapshot_timestamp"] = pd.Timestamp.now()

        if df.empty:
            logger.error("Transformed DataFrame is empty!")
            raise ValueError("No data after transformation")

        if df["id"].isnull().any():
            logger.warning("Some records have missing coin IDs")

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_file = PROCESSED_DATA_DIR / f"cleaned_crypto_data_{timestamp}.csv"

        df.to_csv(output_file, index=False)

        logger.info(f"Processed data saved to: {output_file}")
        logger.info(f"Transformed DataFrame shape: {df.shape}")
        logger.info(f"Total rows after transformation: {len(df)}")

        return output_file

    except Exception as e:
        logger.error(f"Error during transformation: {e}")
        raise


if __name__ == "__main__":
    transform_data()