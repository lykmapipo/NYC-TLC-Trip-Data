"""
Sample NYC TLC trips data from AWS S3 Bucket or NYC TLC Trip Data website.

Usage::

    mkdir -p data/trips-data
    pip install click joblib numpy pyarrow pandas
    python sample_trips_data.py -s web -t yellow -y 2023 -m 1 -f csv

"""

import logging
import re
from pathlib import Path

import click
import pyarrow as pa  # noqa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
from joblib import Parallel, delayed

import conf
from base import configure_logging, default_rng
from extract_trips_data import _discover_trips_metadata


def _write_sample_data(table=None, destination=None, **kwargs):
    """Write sampled data to a file."""
    output_format = kwargs.get("output_format") or "parquet"
    if output_format == "csv":  # write csv
        pcsv.write_csv(
            table,
            str(destination),
        )
    else:  # write parquet
        pq.write_table(
            table,
            str(destination),
            compression=kwargs.get("compression", "snappy"),
            write_statistics=kwargs.get("write_statistics", True),
            flavor=kwargs.get("flavor", "spark"),
            store_schema=kwargs.get("store_schema", True),
        )


def _sample_trip_data(fragment=None, **kwargs):
    """Sample trip file data from AWS S3 bucket or AWS CloudFront."""
    configure_logging()

    # prepare destination
    fragment_file_name = Path(fragment.path).name
    file_parts = re.split(r"[_.-]", fragment_file_name)
    output_format = kwargs.get("output_format") or "parquet"
    logging.info(f"Output format {output_format}")
    destination = f"{file_parts[0]}_tripdatasample_{file_parts[2]}-{file_parts[3]}.{output_format}"  # noqa E501
    destination = Path(conf.DATASET_LOCAL_TRIPS_DATA_SAMPLE_PATH / destination)
    destination = destination.expanduser().resolve()
    destination.parent.mkdir(exist_ok=True, parents=True)

    # sample data randomly from a trip file
    logging.info(f"Sampling {fragment_file_name} ...")
    sample_size = kwargs.get("sample_size") or 100
    sample_indices = default_rng.choice(
        fragment.metadata.num_rows,
        size=sample_size,
        replace=False,
    )
    sample_table = fragment.take(sample_indices)
    _write_sample_data(
        table=sample_table,
        destination=destination,
        compression="snappy",
        write_statistics=True,
        flavor="spark",
        store_schema=True,
        **kwargs,
    )
    logging.info(f"Sampling {fragment_file_name} finished.")


def _sample_trips_data(trip_fragments=None, **kwargs):
    """Sample each trip file data."""
    logging.info("Sampling trips data ...")
    Parallel(
        n_jobs=conf.PARALLEL_NUM_JOBS,
        verbose=conf.PARALLEL_VERBOSITY_LEVEL,
    )(
        delayed(_sample_trip_data)(fragment=trip_fragment, **kwargs)
        for trip_fragment in trip_fragments
    )
    logging.info("Sampling trips data finished.")


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
    "--month",
    "-m",
    type=click.IntRange(min=min(conf.RECORD_MONTHS), max=max(conf.RECORD_MONTHS)),
    default=[min(conf.RECORD_MONTHS)],
    show_default=True,
    help="Trip months to extract.",
)
@click.option(
    "--sample-size",
    "-ss",
    type=int,
    default=1000,
    show_default=True,
    help="Sample size to extract.",
)
@click.option(
    "--output-format",
    "-f",
    type=click.Choice(["parquet", "csv"], case_sensitive=False),
    default="parquet",
    show_default=True,
    help="Output file format.",
)
def main(**kwargs):
    """Sample NYC TLC trips data from AWS S3 Bucket or NYC TLC Trip Data website.

    Examples:

    1. Download yellow data sample of january and february 2023 using S3 (recommended):

    >>> python sample_trips_data.py -s s3 -t yellow -y 2023 -m 1 -f csv


    2. Download yellow data sample of january and february 2023 (non-deterministic):

    >>> python sample_trips_data.py -s web -t yellow -y 2023 -m 1 -f csv

    """
    configure_logging()
    logging.info("Start.")

    trip_fragments = _discover_trips_metadata(**kwargs)
    _sample_trips_data(trip_fragments=trip_fragments, **kwargs)

    logging.info("Done.")


if __name__ == "__main__":
    main()
