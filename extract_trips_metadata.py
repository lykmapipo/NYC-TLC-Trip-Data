"""
Extract NYC TLC trips metadata from AWS S3 Bucket or NYC TLC Trip Data website.

Usage::

    mkdir -p data/trips-metadata
    pip install joblib pyarrow pandas requests beautifulsoup4
    python extract_trips_metadata.py -s web -t yellow -y 2024

"""

import datetime
import logging
import os
import re
import time
from pathlib import Path

import click
import pandas as pd
import pyarrow as pa  # noqa
import pyarrow.dataset as pds
import pyarrow.fs as pfs
import requests
from bs4 import BeautifulSoup
from fsspec.implementations.http import HTTPFileSystem
from joblib import Parallel, delayed

from base import ArrowHTTPFileSystem

TODAY = datetime.date.today()

WEB_SOURCE_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
WEB_FILE_URL_CSS_SELECTOR = "a[href*='trip-data']"
WEB_PARALLEL_NUM_JOBS = 1
WEB_REQUEST_DELAY_TIME = abs(WEB_PARALLEL_NUM_JOBS * 3) + 3

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.environ.get("AWS_REGION") or "us-east-1"
AWS_SCHEME = "https"
AWS_REQUEST_TIMEOUT = 6  # TODO: adjust accordingly (16s+)
AWS_CONNECT_TIMEOUT = 3  # TODO: adjust accordingly (16s+)

DATASETS_S3_PATH = "nyc-tlc/trip data/"
DATASETS_S3_BASE_URL = "s3://nyc-tlc/trip data"
DATASETS_CLOUDFRONT_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

DATASETS_BASE_PATH = Path("./data")
DATASETS_METADATA_PATH = DATASETS_BASE_PATH / "trips-metadata"

PARALLEL_NUM_JOBS = -1
PARALLEL_VERBOSITY_LEVEL = 10

LOGGING_LEVEL = logging.INFO
LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

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
RECORD_YEARS = list(range(2009, TODAY.year + 1))
RECORD_MONTHS = list(range(1, 13))

RECORD_SOURCE_WEB = "web"
RECORD_SOURCE_S3 = "s3"
RECORD_SOURCES = [RECORD_SOURCE_WEB, RECORD_SOURCE_S3]

HEADERS = [
    "file_name",
    "file_s3_url",
    "file_cloudfront_url",
    "file_record_type",
    "file_year",
    "file_month",
    "file_modification_time",
    "file_num_rows",
    "file_num_columns",
    "file_column_names",
    "file_size_bytes",
    "file_size_mbs",
    "file_size_gbs",
]


def _configure_logging():
    """Configure basic multiprocessing loggging."""
    if len(logging.getLogger().handlers) == 0:
        logging.basicConfig(level=LOGGING_LEVEL, format=LOGGING_FORMAT)


def _prepare_s3_filesystem():
    """Prepare PyArrow AWS S3 filesystem."""
    logging.info("Prepare AWS S3 filesystems ...")
    s3_fs = pfs.S3FileSystem(
        access_key=AWS_ACCESS_KEY_ID,
        secret_key=AWS_SECRET_ACCESS_KEY,
        region=AWS_REGION,
        scheme=AWS_SCHEME,
        request_timeout=AWS_REQUEST_TIMEOUT,
        connect_timeout=AWS_CONNECT_TIMEOUT,
    )
    logging.info("Prepare AWS S3 filesystems finished.")
    return s3_fs


def _prepare_web_filesystem():
    """Prepare PyArrow HTTP filesystem using fsspec."""
    logging.info("Prepare Web filesystems ...")
    web_fs = ArrowHTTPFileSystem()
    web_fs = pfs.PyFileSystem(pfs.FSSpecHandler(web_fs))
    logging.info("Prepare Web filesystems finished.")
    return web_fs


def _discover_s3_dataset(**kwargs):
    """Discover metadata from AWS S3."""
    s3_fs = _prepare_s3_filesystem()
    ds = pds.dataset(
        DATASETS_S3_PATH,
        format="parquet",
        partitioning="hive",
        filesystem=s3_fs,
    )
    return ds


def _discover_web_dataset(**kwargs):
    """Discover metadata from NYC TLC Trip Data website and AWS CloudFront."""
    logging.info("Requesting web page ...")
    file_urls = requests.get(WEB_SOURCE_URL)
    logging.info("Requesting page finished.")

    logging.info("Parsing web file urls ...")
    file_urls = BeautifulSoup(file_urls.content, "html.parser")
    file_urls = file_urls.select(WEB_FILE_URL_CSS_SELECTOR)
    file_urls = [file_url.get("href").strip() for file_url in file_urls if file_url]
    logging.info("Parsing web file urls finished.")

    web_fs = _prepare_web_filesystem()
    ds = pds.dataset(
        file_urls,
        format="parquet",
        partitioning="hive",
        filesystem=web_fs,
    )
    return ds


def _filter_allowed_fragment(fragment=None, **kwargs):
    """Filter allowed fragments."""
    file_name = fragment.path.split("/")[-1]
    file_parts = re.split(r"[_.-]", file_name)
    file_record_type = file_parts[0]
    file_year = int(file_parts[2])
    file_month = int(file_parts[3])

    is_allowed_fragment = (
        (file_record_type == kwargs.get("record_type"))
        and (file_year == kwargs.get("year"))
        and (file_month in (kwargs.get("months") or []))
    )
    return is_allowed_fragment


def extract_trip_file_metadata(fragment=None, **kwargs):
    """Extract file size and info."""
    fragment_info = fragment.filesystem.get_file_info(fragment.path)

    # file info
    file_name = fragment.path.split("/")[-1]
    file_s3_url = f"{DATASETS_S3_BASE_URL}/{file_name}"
    file_cloudfront_url = f"{DATASETS_CLOUDFRONT_BASE_URL}/{file_name}"
    file_parts = re.split(r"[_.-]", file_name)
    file_record_type = file_parts[0]
    file_infos = [file_name, file_s3_url, file_cloudfront_url, file_record_type]

    # time info
    file_year = int(file_parts[2])
    file_month = int(file_parts[3])
    file_modification_time = fragment_info.mtime
    time_infos = [file_year, file_month, file_modification_time]

    # size info
    file_size_bytes = int(fragment_info.size)
    file_size_mbs = file_size_bytes / (1024**2)
    file_size_gbs = file_size_bytes / (1024**3)
    file_num_rows = fragment.metadata.num_rows
    file_num_columns = fragment.metadata.num_columns
    file_column_names = ",".join(fragment.metadata.schema.names)
    size_infos = [
        file_num_rows,
        file_num_columns,
        file_column_names,
        file_size_bytes,
        file_size_mbs,
        file_size_gbs,
    ]

    # collect metadata
    file_metadata = file_infos + time_infos + size_infos
    return dict(zip(HEADERS, file_metadata))


@click.command()
@click.option(
    "--source",
    "-s",
    type=click.Choice(RECORD_SOURCES),
    default=RECORD_SOURCE_S3,
    show_default=True,
    help="Trips remote source.",
)
@click.option(
    "--record-type",
    "-t",
    type=click.Choice(RECORD_TYPES),
    default=RECORD_TYPES_YELLOW,
    show_default=True,
    help="Trip record type to extract.",
)
@click.option(
    "--year",
    "-y",
    type=click.IntRange(min=min(RECORD_YEARS), max=max(RECORD_YEARS)),
    default=max(RECORD_YEARS),
    show_default=True,
    help="Trip year to extract.",
)
@click.option(
    "--months",
    "-m",
    multiple=True,
    type=click.IntRange(min=min(RECORD_MONTHS), max=max(RECORD_MONTHS)),
    default=list(RECORD_MONTHS),
    show_default=True,
    help="Trip months to extract.",
)
def main(source=RECORD_SOURCE_S3, record_type=None, year=None, months=None, **kwargs):
    """Extract NYC TLC trips metadata from AWS S3 Bucket or NYC TLC Trip Data website.

    Examples:

    Extracting metadata from AWS S3 Bucket (recommended):
    python extract_trips_metadata.py -s s3 -t yellow -y 2024

    Extracting metadata from NYC TLC Trip Data website:
    python extract_trips_metadata.py -s web -t yellow -y 2024

    """
    _configure_logging()
    logging.info("Start.")

    logging.info("Discovering trips metadata ...")
    trip_dataset = (
        _discover_web_dataset()
        if source == RECORD_SOURCE_WEB
        else _discover_s3_dataset()
    )
    trip_fragments = []
    for trip_fragment in trip_dataset.get_fragments():
        is_allowed_fragment = _filter_allowed_fragment(
            fragment=trip_fragment,
            record_type=record_type,
            year=year,
            months=months,
        )
        if is_allowed_fragment:
            trip_fragments.append(trip_fragment)
    logging.info("Discovering trips metadata finished.")

    logging.info("Extracting trips metadata ...")
    n_jobs = WEB_PARALLEL_NUM_JOBS if source == RECORD_SOURCE_WEB else PARALLEL_NUM_JOBS
    metadata = Parallel(n_jobs=n_jobs, verbose=PARALLEL_VERBOSITY_LEVEL)(
        delayed(extract_trip_file_metadata)(
            fragment=trip_fragment,
            source=source,
            record_type=record_type,
            year=year,
            months=months,
        )
        for trip_fragment in trip_fragments
    )
    metadata_df = pd.DataFrame(metadata)
    logging.info("Extracting trips metadata finished.")

    # save trips metadata
    file_name = f"{TODAY}_{source}_{record_type}_tripmetadata_{year}.csv"
    metadata_pth = DATASETS_METADATA_PATH / file_name
    logging.info(f"Saving trips metadata at {metadata_pth} ...")
    metadata_pth = metadata_pth.expanduser().resolve()
    metadata_pth.parent.mkdir(parents=True, exist_ok=True)
    metadata_df.to_csv(metadata_pth, index=False)
    logging.info("Saving trips metadata finished.")

    logging.info("Done.")


if __name__ == "__main__":
    main()
