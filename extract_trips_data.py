"""
Extract NYC TLC trips data from AWS S3 Bucket or NYC TLC Trip Data website.

Usage::

    mkdir -p data/trips-data
    pip install click joblib pyarrow pandas
    python extract_trips_data.py -s s3 -t yellow -y 2023 -m 1 -m 2

"""

import logging
from pathlib import Path

import click
import pyarrow as pa  # noqa
import pyarrow.fs as pfs
from joblib import Parallel, delayed

import conf
from base import (
    configure_logging,
    discover_dataset,
    is_allowed_fragment,
    prepare_local_fs,
    prepare_s3_fs,
    prepare_web_fs,
)


def _discover_s3_dataset(**kwargs):
    """Discover metadata from AWS S3."""
    s3_fs = prepare_s3_fs()
    ds = discover_dataset(
        source=conf.DATASET_AWS_S3_PATH,
        filesystem=s3_fs,
    )
    return ds


def _discover_web_dataset(**kwargs):
    """Discover metadata from NYC TLC Trip Data website and AWS CloudFront."""
    record_type = kwargs.get("record_type")
    year = kwargs.get("year")
    months = kwargs.get("months") or [kwargs.get("month")]

    file_urls = [
        f"{conf.DATASET_AWS_CLOUDFRONT_BASE_URL}/{url}"
        for url in [
            f"{record_type}_tripdata_{year}-{month:02}.parquet" for month in months
        ]
    ]

    web_fs = prepare_web_fs()
    ds = discover_dataset(
        source=file_urls,
        filesystem=web_fs,
    )
    return ds


def _discover_trips_metadata(**kwargs):
    """Discover trips fragments (files)."""
    logging.info("Discovering trips metadata ...")

    source = kwargs.get("source")
    trip_dataset = (
        _discover_s3_dataset()
        if source == conf.RECORD_SOURCE_S3
        else _discover_web_dataset(**kwargs)
    )
    trip_fragments = []
    for trip_fragment in trip_dataset.get_fragments():
        allowed_fragment = is_allowed_fragment(
            fragment=trip_fragment,
            **kwargs,
        )
        if allowed_fragment:
            trip_fragments.append(trip_fragment)
    logging.info("Discovering trips metadata finished.")
    return trip_fragments


def _extract_trip_data(fragment=None, **kwargs):
    """Download trip file data from AWS S3 bucket or AWS CloudFront."""
    configure_logging()

    # ensure source and local filesystems
    source_fs = fragment.filesystem
    local_fs = prepare_local_fs()

    # prepare source and destination
    source = fragment.path
    destination = Path(fragment.path).name
    destination = Path(conf.DATASET_LOCAL_TRIPS_DATA_PATH / destination)
    destination = destination.expanduser().resolve()
    destination.parent.mkdir(exist_ok=True, parents=True)
    logging.info(f"Downloading {destination.name} ...")

    # fetch local and source file modification time
    source_file_info = source_fs.get_file_info(fragment.path)
    source_modification_time = source_file_info.mtime
    destination_exists = destination.exists()
    local_modification_time = None
    if destination_exists:
        local_modification_time = local_fs.get_file_info(str(destination)).mtime

    # check if destination need updates
    destination_need_update = (
        local_modification_time
        and source_modification_time
        and (local_modification_time < source_modification_time)
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
            chunk_size=conf.DOWNLOAD_CHUNK_SIZE,
            use_threads=conf.DOWNLOAD_USE_THREADS,
        )
        logging.info(f"Downloading {destination.name} finished.")
    else:
        logging.info(f"{destination.name} is up to date.")


def _extract_trips_data(trip_fragments=None, **kwargs):
    """Extract each trip file data."""
    logging.info("Extracting trips data ...")
    Parallel(
        n_jobs=conf.PARALLEL_NUM_JOBS,
        verbose=conf.PARALLEL_VERBOSITY_LEVEL,
    )(
        delayed(_extract_trip_data)(fragment=trip_fragment, **kwargs)
        for trip_fragment in trip_fragments
    )
    logging.info("Extracting trips data finished.")


@click.command()
@click.option(
    "--source",
    "-s",
    type=click.Choice(conf.RECORD_SOURCES),
    default=conf.RECORD_SOURCE_WEB,
    show_default=True,
    help="Trips remote source.",
)
@click.option(
    "--record-type",
    "-t",
    type=click.Choice(conf.RECORD_TYPES),
    default=conf.RECORD_TYPES_YELLOW,
    show_default=True,
    help="Trip record type to extract.",
)
@click.option(
    "--year",
    "-y",
    type=click.IntRange(min=min(conf.RECORD_YEARS), max=max(conf.RECORD_YEARS)),
    default=max(conf.RECORD_YEARS),
    show_default=True,
    help="Trip year to extract.",
)
@click.option(
    "--months",
    "-m",
    multiple=True,
    type=click.IntRange(min=min(conf.RECORD_MONTHS), max=max(conf.RECORD_MONTHS)),
    default=[min(conf.RECORD_MONTHS)],
    show_default=True,
    help="Trip months to extract.",
)
def main(**kwargs):
    """Extract NYC TLC trips data from AWS S3 Bucket or NYC TLC Trip Data website.

    Examples:

    1. Download yellow data of january and february 2023 using S3 (recommended):

    >>> python extract_trips_data.py -s s3 -t yellow -y 2023 -m 1 -m 2


    2. Download yellow data of january and february 2023 (non-deterministic):

    >>> python extract_trips_data.py -s web -t yellow -y 2023 -m 1 -m 2

    """
    configure_logging()
    logging.info("Start.")

    trip_fragments = _discover_trips_metadata(**kwargs)
    _extract_trips_data(trip_fragments=trip_fragments, **kwargs)

    logging.info("Done.")


if __name__ == "__main__":
    main()
