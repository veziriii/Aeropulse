import os
import requests
import logging
from pathlib import Path
from dotenv import load_dotenv
import gzip
import json


load_dotenv()

LOG_DIR = os.getenv("LOG_DIR")  
Path(LOG_DIR).mkdir(parents=True, exist_ok=True)  
LOG_FILE = Path(LOG_DIR) / "download_city_list.log"

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR")
Path(DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=10,
    filename=LOG_FILE,
    encoding="utf-8",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M",
)

def save_bulk_cities_data(url: str = "http://bulk.openweathermap.org/sample/city.list.json.gz"):
    """
    Downloads the gzipped city list (with city names, latitude, longitude)
    and saves to DOWNLOAD_DIR
    Arg:
    url (str): url of bulk city zip file in openweather data source
    
    Returns:
        file path of the downloaded gzip file 
    """

    try:
        logging.info(f"Starting download from {url}")
        response = requests.get(url, timeout=30)

        if response.status_code == 200:
            file_path = Path(DOWNLOAD_DIR) / "city_list_json.gz"
            with open(file_path, "wb") as f:
                f.write(response.content)

            logging.info(f"Successfully downloaded {file_path} ({len(response.content)} bytes)")
            return file_path
        else:
            logging.error(f"Failed to download. Status code: {response.status_code}")
            return None

    except Exception as e:
        logging.exception(f"Error occurred while downloading: {e}")
        return None


def extract_gzip_to_json(gz_path , json_path = None):
    """
    Extracts a gzip-compressed JSON file (.gz) into a readable JSON file.

    Args:
        gz_path : Path to the .gz file
        json_path : Path to the returning json file 
                         If None, uses the same name as gz_path but with .json extension.

    Returns:
        Path: The path to the saved JSON file
    """

    gz_file = Path(gz_path)

    if json_path is None:
        json_path = gz_file.with_suffix("")
        json_path = json_path.with_suffix(".json")

    with gzip.open(gz_file, "rt", encoding="utf-8") as f:
        data = json.load(f)

    with open(json_path, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, indent=2, ensure_ascii=False)

    return Path(json_path)


if __name__ == "__main__":
    gz_file = save_bulk_cities_data()
    if gz_file:
        json_file = extract_gzip_to_json(gz_file)
        print(f"Extracted JSON saved at: {json_file}")
    else:
        print("Download failed. Check logs for details.")

