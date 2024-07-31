"""
Extract NYC TLC trips data from AWS S3 Bucket or NYC TLC Trip Data website.

Usage::

    mkdir -p data/trips-data
    pip install click joblib pyarrow pandas
    python extract_trips_data.py -s s3 -t yellow -y 2023 -m 1 -m 2

"""

import datetime
import logging
import os
import re
from pathlib import Path

import click
import pyarrow as pa  # noqa
import pyarrow.dataset as pds
import pyarrow.fs as pfs
from fsspec.implementations.http import HTTPFileSystem
from joblib import Parallel, delayed

TODAY = datetime.date.today()

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.environ.get("AWS_REGION") or "us-east-1"
AWS_SCHEME = "https"
AWS_REQUEST_TIMEOUT = 6  # TODO: adjust accordingly (16s+)
AWS_CONNECT_TIMEOUT = 3  # TODO: adjust accordingly (16s+)

DATASETS_FORMAT = "parquet"
DATASETS_PARTITIONING = "hive"
DATASETS_S3_PATH = "nyc-tlc/trip data/"
DATASETS_CLOUDFRONT_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

DATASETS_BASE_PATH = Path("./data")
DATASETS_TRIPS_DATA_PATH = DATASETS_BASE_PATH / "trips-data"

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

DOWNLOAD_CHUNK_SIZE = 1048576  # 1MB
DOWNLOAD_USE_THREADS = True


# init
def _configure_logging():
    """Configure basic multiprocessing loggging."""
    if len(logging.getLogger().handlers) == 0:
        logging.basicConfig(level=LOGGING_LEVEL, format=LOGGING_FORMAT)


def _prepare_source_fs(source=None):
    """Prepare PyArrow source filesystem."""
    if source == RECORD_SOURCE_WEB:
        web_fs = HTTPFileSystem()
        web_fs = pfs.PyFileSystem(pfs.FSSpecHandler(web_fs))
        return web_fs

    s3_fs = pfs.S3FileSystem(
        access_key=AWS_ACCESS_KEY_ID,
        secret_key=AWS_SECRET_ACCESS_KEY,
        region=AWS_REGION,
        scheme=AWS_SCHEME,
        request_timeout=AWS_REQUEST_TIMEOUT,
        connect_timeout=AWS_CONNECT_TIMEOUT,
    )
    return s3_fs


def _prepare_sources(source=None, record_type=None, year=None, months=None):
    """Prepare PyArrow source paths."""
    if source == RECORD_SOURCE_WEB:
        web_paths = [
            f"{DATASETS_CLOUDFRONT_BASE_URL}/{record_type}_tripdata_{year}-{month:02}.parquet"
            for month in months
        ]
        return web_paths

    return DATASETS_S3_PATH


def download_trip_file(
    fragment=None, fragment_file_info=None, source_fs=None, local_fs=None, **kwargs
):
    """Download trip file data from AWS S3 bucket."""
    _configure_logging()

    # prepare source and destination
    source = fragment.path
    destination = Path(fragment.path).name
    destination = Path(DATASETS_TRIPS_DATA_PATH / destination)
    destination = destination.expanduser().resolve()
    destination.parent.mkdir(exist_ok=True, parents=True)
    logging.info(f"Downloading {destination.name} ...")

    # fetch local and s3 file modification time
    s3_modification_time = fragment_file_info.mtime
    destination_exists = destination.exists()
    local_modification_time = (
        local_fs.get_file_info(str(destination)).mtime if destination_exists else None
    )
    destination_need_update = (
        local_modification_time
        and s3_modification_time
        and (local_modification_time < s3_modification_time)
    )

    # try download trip file
    if not destination_exists or destination_need_update:
        if not destination_exists:
            logging.info(f"{destination.name} does not exists. Try downloading ...")
        if destination_need_update:
            logging.info(f"{destination.name} already exists. Try updating ...")
        pfs.copy_files(
            source,
            destination,
            source_filesystem=source_fs,
            destination_filesystem=local_fs,
            chunk_size=DOWNLOAD_CHUNK_SIZE,
            use_threads=DOWNLOAD_USE_THREADS,
        )
        logging.info(f"Downloading {destination.name} finished.")
    else:
        logging.info(f"{destination.name} is up to date.")


def extract_trip_file(fragment=None, **kwargs):
    """Download trip file data from AWS S3 bucket."""
    _configure_logging()

    # extract fragment metadata
    file_name = fragment.path.split("/")[-1]
    file_parts = re.split(r"[_.-]", file_name)
    file_record_type = file_parts[0]
    file_year = int(file_parts[2])
    file_month = int(file_parts[3])

    # TODO: more filters

    # download fragment file
    download_fragment = (
        (file_record_type == kwargs.get("record_type"))
        and (file_year == kwargs.get("year"))
        and (file_month in (kwargs.get("months") or []))
    )
    if download_fragment:
        fragment_file_info = fragment.filesystem.get_file_info(fragment.path)
        download_trip_file(
            fragment=fragment, fragment_file_info=fragment_file_info, **kwargs
        )


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
    default=[min(RECORD_MONTHS)],
    show_default=True,
    help="Trip months to extract.",
)
def main(source=RECORD_SOURCE_S3, record_type=None, year=None, months=None, **kwargs):
    """Extract NYC TLC trips data from AWS S3 Bucket or or NYC TLC Trip Data website.

    Examples:

    Download yellow data of january and february 2023 using S3 (recommended):
    python extract_trips_data.py -s s3 -t yellow -y 2023 -m 1 -m 2


    Download yellow data of january and february 2023 (non-deterministic):
    python extract_trips_data.py -s web -t yellow -y 2023 -m 1 -m 2

    """
    _configure_logging()
    logging.info("Start.")

    logging.info("Prepare filesystems ...")
    source_fs = _prepare_source_fs(source=source)
    local_fs = pfs.LocalFileSystem()
    logging.info("Prepare filesystems finished.")

    logging.info("Discovering trips metadata ...")
    sources = _prepare_sources(
        source=source, record_type=record_type, year=year, months=months
    )
    trip_dataset = pds.dataset(
        sources,
        format=DATASETS_FORMAT,
        partitioning=DATASETS_PARTITIONING,
        filesystem=source_fs,
    )
    trip_fragments = trip_dataset.get_fragments()
    logging.info("Discovering trips metadata finished.")

    logging.info("Extracting trips data ...")
    Parallel(n_jobs=PARALLEL_NUM_JOBS, verbose=PARALLEL_VERBOSITY_LEVEL)(
        delayed(extract_trip_file)(
            fragment=trip_fragment,
            record_type=record_type,
            year=year,
            months=months,
            source_fs=source_fs,
            local_fs=local_fs,
        )
        for trip_fragment in trip_fragments
    )
    logging.info("Extracting trips data finished.")

    logging.info("Done.")


if __name__ == "__main__":
    main()
