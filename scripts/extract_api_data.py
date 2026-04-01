import os
import json
import logging
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Define directories
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
LOGS_DIR = BASE_DIR / "logs"

# Ensure directories exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logs_file = LOGS_DIR / "extract.log"

#configure logging
logging.basicConfig(
    level=logging.INFO,
    format= "%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(logs_file),
        logging.StreamHandler()
    ]
)



# function to extract data from CoinGecko API
def extract_data():
    api_base_url = os.getenv("API_BASE_URL")
    vs_currency = os.getenv("VS_CURRENCY", "usd")
    per_page = os.getenv("PER_PAGE", "50")
    page = os.getenv("PAGE", "1")

    if not api_base_url:
        logging.error("API_BASE_URL is missing in the .env file")
        raise ValueError("API_BASE_URL is missing in the .env file")
    
    endpoint = f"{api_base_url}/coins/markets"

    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": page,
        "sparkline": "false"
    }

    try:
        logging.info(f"Sending request to {endpoint}")
        response = requests.get(endpoint, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_path = RAW_DATA_DIR / f"extracted_crypto_market_data on {timestamp}.json"
        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)

            logging.info(f"Raw data successfully extracted and saved to {output_path}")
            logging.info(f"Number of records extracted: {len(data)}")

            return output_path
        
    except requests.exceptions.RequestException as e:
            logging.error(f"API request failed: {e}")
            raise
    
    except Exception as e:
         logging.error(f"Unexpected error during data extraction: {e}")
         raise
    

if __name__ == "__main__":
     extract_data()



        



