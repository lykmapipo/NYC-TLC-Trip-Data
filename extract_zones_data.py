"""
Extract NYC TLC zone maps and lookup tables from the website.

Usage::

    mkdir -p data/zones-data
    pip install requests pandas geopandas
    python extract_zones_data.py

"""

import datetime
import logging
from pathlib import Path
from urllib.parse import urlparse

import requests

import conf
from base import configure_logging


def download_file(url=None, destination=None, chunk_size=1024, **kwargs):
    """Download a file from a given url."""
    logging.info(f"Downloading {url} ...")

    with requests.get(url, stream=True, **kwargs) as response:
        response.raise_for_status()

        with destination.open("wb") as file:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    file.write(chunk)

    logging.info(f"Downloading {url} finished.")


def safe_download_file(url=None, **kwargs):
    """Download a file only if it doesn't exist or it has been updated remotely."""
    destination = Path(urlparse(url).path).name
    destination = Path(conf.DATASET_LOCAL_ZONES_DATA_PATH / destination)
    destination = destination.expanduser().resolve()
    destination.parent.mkdir(exist_ok=True, parents=True)

    # file not exists, try download
    if not destination.exists():
        logging.info(f"{destination.name} does not exists. Try downloading ...")
        download_file(url=url, destination=destination, **kwargs)

    # file exists, try update
    else:
        logging.info(f"{destination.name} already exists. Try updating ...")
        local_last_modified_time = datetime.datetime.utcfromtimestamp(
            destination.lstat().st_mtime
        )

        server_last_modified_time = local_last_modified_time
        response = requests.head(url)
        if response.status_code == 200:
            server_last_modified_time = datetime.datetime.strptime(
                response.headers["Last-Modified"], "%a, %d %b %Y %H:%M:%S %Z"
            )

        if server_last_modified_time > local_last_modified_time:
            download_file(url=url, destination=destination, **kwargs)
        else:
            logging.info(f"{destination.name} is up to date.")


configure_logging()
logging.info("Start.")

logging.info("Downloading taxi zone lookup file ...")
safe_download_file(conf.DATASET_ZONE_LOOKUP_DOWNLOAD_URL)
logging.info("Downloading taxi zone lookup file finished.")

logging.info("Downloading taxi zones file ...")
safe_download_file(conf.DATASET_ZONES_DOWNLOAD_URL)
logging.info("Downloading taxi zones file finished.")

logging.info("Done.")
