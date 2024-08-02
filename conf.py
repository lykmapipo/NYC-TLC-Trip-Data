"""
Common settings.

"""

import datetime
import logging
import os
from pathlib import Path

TODAY = datetime.date.today()
CURRENT_YEAR = TODAY.year
CURRENT_MONTH = TODAY.month

LOGGING_LEVEL = logging.INFO
LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

PARALLEL_NUM_JOBS = -1
PARALLEL_VERBOSITY_LEVEL = 10

DOWNLOAD_CHUNK_SIZE = 1048576  # 1MB
DOWNLOAD_USE_THREADS = True

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.environ.get("AWS_REGION") or "us-east-1"
AWS_SCHEME = "https"
AWS_REQUEST_TIMEOUT = 6  # TODO: adjust accordingly (16s+)
AWS_CONNECT_TIMEOUT = 3  # TODO: adjust accordingly (16s+)

DATASET_FORMAT = "parquet"
DATASET_PARTITIONING = "hive"

DATASET_LOCAL_BASE_PATH = Path("./data")
DATASET_LOCAL_METADATA_PATH = DATASET_LOCAL_BASE_PATH / "trips-metadata"
DATASET_LOCAL_TRIPS_DATA_PATH = DATASET_LOCAL_BASE_PATH / "trips-data"

DATASET_WEB_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
DATASET_WEB_FILE_URL_CSS_SELECTOR = "a[href*='trip-data']"

DATASET_AWS_S3_PATH = "nyc-tlc/trip data/"
DATASET_AWS_S3_BASE_URL = "s3://nyc-tlc/trip data"
DATASET_AWS_CLOUDFRONT_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

RECORD_SOURCE_WEB = "web"
RECORD_SOURCE_S3 = "s3"
RECORD_SOURCES = [RECORD_SOURCE_WEB, RECORD_SOURCE_S3]

RECORD_TYPES_FHV = "fhv"
RECORD_TYPES_FHVHV = "fhvhv"
RECORD_TYPES_GREEN = "green"
RECORD_TYPES_YELLOW = "yellow"
RECORD_TYPES = [
    RECORD_TYPES_FHV,
    RECORD_TYPES_FHVHV,
    RECORD_TYPES_GREEN,
    RECORD_TYPES_YELLOW,
]

RECORD_YEARS = list(range(2009, CURRENT_YEAR + 1))
RECORD_MONTHS = list(range(1, 13))
