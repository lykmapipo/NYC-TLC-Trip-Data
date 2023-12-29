"""
Extract NYC TLC trips data from AWS S3 Bucket.

Usage::

    mkdir -p data/trips-data
    pip install click joblib pyarrow pandas
    python extract_trips_data.py -t yellow -y 2023 -m 1 -m 2

"""

import logging
import os
import re
from pathlib import Path

import click
import pyarrow as pa  # noqa
import pyarrow.dataset as pds
import pyarrow.fs as pfs
from joblib import Parallel, delayed

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.environ.get("AWS_REGION") or "us-east-1"
AWS_SCHEME = "https"
AWS_REQUEST_TIMEOUT = 6  # TODO: adjust accordingly (16s+)
AWS_CONNECT_TIMEOUT = 3  # TODO: adjust accordingly (16s+)

DATASETS_FORMAT = "parquet"
DATASETS_PARTITIONING = "hive"
DATASETS_S3_PATH = "nyc-tlc/trip data/"

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
RECORD_TYPES = [RECORD_TYPES_FHV, RECORD_TYPES_FHVHV, RECORD_TYPES_GREEN, RECORD_TYPES_YELLOW]
RECORD_YEARS = list(range(2015, 2024))
RECORD_MONTHS = list(range(1, 13))

DOWNLOAD_CHUNK_SIZE = 1048576  # 1MB
DOWNLOAD_USE_THREADS = True


# init
def _configure_logging():
    """Configure basic multiprocessing loggging."""
    if len(logging.getLogger().handlers) == 0:
        logging.basicConfig(level=LOGGING_LEVEL, format=LOGGING_FORMAT)


def download_trip_file(fragment=None, fragment_file_info=None, s3_fs=None, local_fs=None):
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
    local_modification_time = local_fs.get_file_info(str(destination)).mtime if destination_exists else None
    destination_need_update = local_modification_time and (local_modification_time < s3_modification_time)

    # try download trip file
    if not destination_exists or destination_need_update:
        if not destination_exists:
            logging.info(f"{destination.name} does not exists. Try downloading ...")
        if destination_need_update:
            logging.info(f"{destination.name} already exists. Try updating ...")
        pfs.copy_files(
            source,
            destination,
            source_filesystem=s3_fs,
            destination_filesystem=local_fs,
            chunk_size=DOWNLOAD_CHUNK_SIZE,
            use_threads=DOWNLOAD_USE_THREADS,
        )
        logging.info(f"Downloading {destination.name} finished.")
    else:
        logging.info(f"{destination.name} is up to date.")


def extract_trip_file(fragment=None, record_types=None, years=None, months=None, **kwargs):
    """Download trip file data from AWS S3 bucket."""
    _configure_logging()

    # initialize filters
    record_types = record_types or []
    years = years or []
    months = months or []

    # extract fragment metadata
    fragment_name = fragment.path.split("/")[-1]
    fragment_parts = re.split(r"[_.-]", fragment_name)
    record_type = fragment_parts[0]
    year = int(fragment_parts[2])
    month = int(fragment_parts[3])

    # TODO: more filters

    # download fragment file
    download_fragment = (record_type in record_types) and (year in years) and (month in months)
    if download_fragment:
        fragment_file_info = fragment.filesystem.get_file_info(fragment.path)
        download_trip_file(fragment=fragment, fragment_file_info=fragment_file_info, **kwargs)


@click.command()
@click.option(
    "--record-types",
    "-t",
    multiple=True,
    type=click.Choice(RECORD_TYPES),
    default=[RECORD_TYPES_YELLOW],
    show_default=True,
    help="Trip record types to extract.",
)
@click.option(
    "--years",
    "-y",
    multiple=True,
    type=click.IntRange(min=min(RECORD_YEARS), max=max(RECORD_YEARS)),
    default=[max(RECORD_YEARS)],
    show_default=True,
    help="Trip years to extract.",
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
def main(record_types=None, years=None, months=None):
    """Extract NYC TLC trips data from AWS S3 Bucket.

    Examples:

    To download yellow taxi data of january and february 2023:\n
    python extract_trips_data.py -t yellow -y 2023 -m 1 -m 2

    """
    _configure_logging()
    logging.info("Start.")

    logging.info("Prepare filesystems ...")
    s3_fs = pfs.S3FileSystem(
        access_key=AWS_ACCESS_KEY_ID,
        secret_key=AWS_SECRET_ACCESS_KEY,
        region=AWS_REGION,
        scheme=AWS_SCHEME,
        request_timeout=AWS_REQUEST_TIMEOUT,
        connect_timeout=AWS_CONNECT_TIMEOUT,
    )
    local_fs = pfs.LocalFileSystem()
    logging.info("Prepare filesystems finished.")

    logging.info("Discovering trips metadata ...")
    trip_dataset = pds.dataset(
        DATASETS_S3_PATH,
        format=DATASETS_FORMAT,
        partitioning=DATASETS_PARTITIONING,
        filesystem=s3_fs,
    )
    trip_fragments = trip_dataset.get_fragments()
    logging.info("Discovering trips metadata finished.")

    logging.info("Extracting trips data ...")
    Parallel(n_jobs=PARALLEL_NUM_JOBS, verbose=PARALLEL_VERBOSITY_LEVEL)(
        delayed(extract_trip_file)(
            fragment=trip_fragment,
            record_types=record_types,
            years=years,
            months=months,
            s3_fs=s3_fs,
            local_fs=local_fs,
        )
        for trip_fragment in trip_fragments
    )
    logging.info("Extracting trips data finished.")

    logging.info("Done.")


if __name__ == "__main__":
    main()
