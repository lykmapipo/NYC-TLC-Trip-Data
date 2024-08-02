"""
Extract NYC TLC trips metadata from AWS S3 Bucket or NYC TLC Trip Data website.

Usage::

    mkdir -p data/trips-metadata
    pip install joblib pyarrow pandas requests beautifulsoup4
    python extract_trips_metadata.py -s web -t yellow -y 2024

"""

import logging
import re

import click
import pandas as pd
import pyarrow as pa  # noqa
from joblib import Parallel, delayed

import conf
from base import (
    configure_logging,
    discover_dataset,
    is_allowed_fragment,
    prepare_s3_fs,
    prepare_web_fs,
    scrape_web_file_urls,
)

METADATA_HEADERS = [
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
    "file_metadata_source",
]


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
    file_urls = scrape_web_file_urls()
    file_urls = [
        file_url
        for file_url in file_urls
        if is_allowed_fragment(fragment=file_url, **kwargs)
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
        _discover_s3_dataset(**kwargs)
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


def _extract_trip_metadata(fragment=None, **kwargs):
    """Extract trip file metadata."""
    fragment_info = fragment.filesystem.get_file_info(fragment.path)

    # file info
    file_name = fragment.path.split("/")[-1]
    file_s3_url = f"{conf.DATASET_AWS_S3_BASE_URL}/{file_name}"
    file_cloudfront_url = f"{conf.DATASET_AWS_CLOUDFRONT_BASE_URL}/{file_name}"
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
    file_metadata_source = kwargs.get("source") or None
    size_infos = [
        file_num_rows,
        file_num_columns,
        file_column_names,
        file_size_bytes,
        file_size_mbs,
        file_size_gbs,
        file_metadata_source,
    ]

    # collect trip file metadata
    file_metadata = file_infos + time_infos + size_infos
    file_metadata = dict(zip(METADATA_HEADERS, file_metadata))
    return file_metadata


def _extract_trips_metadata(trip_fragments=None, **kwargs):
    """Extract each trip file metadata."""
    logging.info("Extracting trips metadata ...")
    trips_metadata = Parallel(
        n_jobs=conf.PARALLEL_NUM_JOBS,
        verbose=conf.PARALLEL_VERBOSITY_LEVEL,
    )(
        delayed(_extract_trip_metadata)(fragment=trip_fragment, **kwargs)
        for trip_fragment in trip_fragments
    )
    logging.info("Extracting trips metadata finished.")
    return trips_metadata


def _save_trips_metadata(trips_metadata=None, **kwargs):
    """Save trips metadata into a file."""
    record_type = kwargs.get("record_type")
    year = kwargs.get("year")

    file_name = f"{record_type}_tripmetadata_{year}.csv"
    file_path = conf.DATASET_LOCAL_METADATA_PATH / file_name
    logging.info(f"Saving trips metadata at {file_path} ...")
    file_path = file_path.expanduser().resolve()
    file_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(trips_metadata)
    df.to_csv(file_path, index=False)
    logging.info("Saving trips metadata finished.")


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
    default=list(conf.RECORD_MONTHS),
    show_default=True,
    help="Trip months to extract.",
)
def main(**kwargs):
    """Extract NYC TLC trips metadata from AWS S3 Bucket or NYC TLC Trip Data website.

    Examples:

    1. Extracting metadata from AWS S3 Bucket (recommended):

    >>> python extract_trips_metadata.py -s s3 -t yellow -y 2024

    2. Extracting metadata from NYC TLC Trip Data website:

    >>> python extract_trips_metadata.py -s web -t yellow -y 2024

    """
    configure_logging()
    logging.info("Start.")

    trip_fragments = _discover_trips_metadata(**kwargs)
    trips_metadata = _extract_trips_metadata(
        trip_fragments=trip_fragments,
        **kwargs,
    )
    _save_trips_metadata(trips_metadata=trips_metadata, **kwargs)

    logging.info("Done.")


if __name__ == "__main__":
    main()
