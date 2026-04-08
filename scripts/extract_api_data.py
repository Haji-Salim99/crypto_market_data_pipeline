import os
import json
import logging
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv


load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
LOGS_DIR = BASE_DIR / "logs"

RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

log_file = LOGS_DIR / "extract.log"
# Set up logging
logger = logging.getLogger("extract_logger")
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


def extract_data():
    api_base_url = os.getenv("API_BASE_URL")
    vs_currency = os.getenv("VS_CURRENCY", "usd")
    per_page = os.getenv("PER_PAGE", "50")
    page = os.getenv("PAGE", "1")

    if not api_base_url:
        logger.error("API_BASE_URL is missing in the .env file.")
        raise ValueError("API_BASE_URL is missing in the .env file.")

    endpoint = f"{api_base_url}/coins/markets"

    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": page,
        "sparkline": "false"
    }

    try:
        logger.info("Sending request to CoinGecko API...")
        response = requests.get(endpoint, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_path = RAW_DATA_DIR / f"crypto_market_{timestamp}.json"

        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)

        logger.info(f"Raw data saved to: {output_path}")
        logger.info(f"Number of records fetched: {len(data)}")

        return output_path

    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise

    except Exception as e:
        logger.error(f"Unexpected error during extraction: {e}")
        raise


if __name__ == "__main__":
    extract_data()