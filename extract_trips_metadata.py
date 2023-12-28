"""
Extract NYC TLC trips metadata from AWS S3 Bucket.

Usage::

    mkdir -p data/trips-metadata
    pip install joblib pyarrow pandas
    python extract_trips_metadata.py

"""

import datetime
import logging
import os
import re
from pathlib import Path

import pandas as pd
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

DATASETS_S3_PATH = "nyc-tlc/trip data/"
DATASETS_BASE_PATH = Path("./data")
DATASETS_METADATA_PATH = DATASETS_BASE_PATH / "trips-metadata"

PARALLEL_NUM_JOBS = -1
PARALLEL_VERBOSITY_LEVEL = 10

LOGGING_LEVEL = logging.INFO
LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

HEADERS = [
    "file_path",
    "file_name",
    # "file_s3_url",
    # "file_cloudfront_url",
    "file_record_type",
    "file_year",
    "file_month",
    # "file_last_modified_time",
    "file_num_rows",
    "file_size_bytes",
    "file_size_mbs",
    "file_size_gbs",
]

# init
logging.basicConfig(level=LOGGING_LEVEL, format=LOGGING_FORMAT)

# discovering trips dataset
logging.info("Start.")
logging.info("Discovering trips metadata ...")
s3 = pfs.S3FileSystem(
    access_key=AWS_ACCESS_KEY_ID,
    secret_key=AWS_SECRET_ACCESS_KEY,
    region=AWS_REGION,
    scheme=AWS_SCHEME,
    request_timeout=AWS_REQUEST_TIMEOUT,
    connect_timeout=AWS_CONNECT_TIMEOUT,
)
trip_ds = pds.dataset(DATASETS_S3_PATH, format="parquet", partitioning="hive", filesystem=s3)
trip_fragments = trip_ds.get_fragments()
logging.info("Discovering trips metadata finished.")

# extract trips metadata
logging.info("Extracting trips metadata ...")


def extract_trip_file_metadata(trip_fragment):
    """Extract file size and info."""
    # file info
    file_path = trip_fragment.path
    file_name = file_path.split("/")[-1]
    file_parts = re.split(r"[_.-]", file_name)
    file_record_type = file_parts[0]
    file_infos = [file_path, file_name, file_record_type]

    # time info
    file_year = int(file_parts[2])
    file_month = int(file_parts[3])
    # file_last_modified_time = None
    time_infos = [file_year, file_month]

    # size info
    trip_fragment_info = trip_fragment.filesystem.get_file_info(trip_fragment.path)
    file_size_bytes = int(trip_fragment_info.size)
    file_size_mbs = file_size_bytes / (1024**2)
    file_size_gbs = file_size_bytes / (1024**3)
    file_num_rows = trip_fragment.metadata.num_rows
    size_infos = [file_num_rows, file_size_bytes, file_size_mbs, file_size_gbs]

    # collect metadata
    file_metadata = file_infos + time_infos + size_infos
    return dict(zip(HEADERS, file_metadata))


data = Parallel(n_jobs=PARALLEL_NUM_JOBS, verbose=PARALLEL_VERBOSITY_LEVEL)(
    delayed(extract_trip_file_metadata)(trip_fragment) for trip_fragment in trip_fragments
)
df = pd.DataFrame(data)
logging.info("Extracting trips metadata finished.")

# save trips metadata
extraction_date = datetime.date.today()
metadata_pth = DATASETS_METADATA_PATH / f"{extraction_date}.csv"
logging.info(f"Saving trips metadata at {metadata_pth} ...")
metadata_pth = metadata_pth.expanduser().resolve()
metadata_pth.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(metadata_pth, index=False)
logging.info("Saving trips metadata finished.")

# pa._s3fs.finalize_s3()
logging.info("Done.")
