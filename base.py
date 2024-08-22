"""
Re-usable utilities.

"""

import logging
import re
from email.utils import parsedate_to_datetime

import numpy as np
import pyarrow.dataset as pds
import pyarrow.fs as pfs
import requests
from bs4 import BeautifulSoup
from fsspec.implementations.http import HTTPFileSystem

import conf

__all__ = [
    "ArrowHTTPFileSystem",
    "configure_logging",
    "default_rng",
    "discover_dataset",
    "is_allowed_fragment",
    "prepare_local_fs",
    "prepare_s3_fs",
    "prepare_web_fs",
    "scrape_web_file_urls",
]


default_rng = np.random.default_rng(conf.RANDOM_SEED)


def configure_logging():
    """Configure basic logging."""
    if len(logging.getLogger().handlers) == 0:
        logging.basicConfig(
            level=conf.LOGGING_LEVEL,
            format=conf.LOGGING_FORMAT,
        )


def prepare_local_fs():
    """Prepare PyArrow local filesystem."""
    logging.info("Prepare local filesystem ...")
    local_fs = pfs.LocalFileSystem()
    logging.info("Prepare local filesystem finished.")
    return local_fs


def prepare_s3_fs():
    """Prepare PyArrow AWS S3 filesystem."""
    logging.info("Prepare AWS S3 filesystem ...")
    s3_fs = pfs.S3FileSystem(
        access_key=conf.AWS_ACCESS_KEY_ID,
        secret_key=conf.AWS_SECRET_ACCESS_KEY,
        region=conf.AWS_REGION,
        scheme=conf.AWS_SCHEME,
        request_timeout=conf.AWS_REQUEST_TIMEOUT,
        connect_timeout=conf.AWS_CONNECT_TIMEOUT,
    )
    logging.info("Prepare AWS S3 filesystem finished.")
    return s3_fs


def prepare_web_fs():
    """Prepare PyArrow HTTP filesystem using fsspec."""
    logging.info("Prepare Web filesystem ...")
    web_fs = ArrowHTTPFileSystem()
    web_fs = pfs.PyFileSystem(pfs.FSSpecHandler(web_fs))
    logging.info("Prepare Web filesystem finished.")
    return web_fs


def discover_dataset(source=None, filesystem=None):
    """Discover PyArrow dataset."""
    logging.info("Discover dataset ...")
    ds = pds.dataset(
        source,
        format=conf.DATASET_FORMAT,
        partitioning=conf.DATASET_PARTITIONING,
        filesystem=filesystem,
    )
    logging.info("Discover dataset finished.")
    return ds


def scrape_web_file_urls():
    """Scrape file urls from NYC TLC Trip Data website."""
    logging.info("Scrape web file urls ...")
    file_urls = requests.get(conf.DATASET_WEB_URL)
    file_urls = BeautifulSoup(file_urls.content, "html.parser")
    file_urls = file_urls.select(conf.DATASET_WEB_FILE_URL_CSS_SELECTOR)
    file_urls = [file_url.get("href").strip() for file_url in file_urls if file_url]
    logging.info("Scrape web file urls finished.")
    return file_urls


def is_allowed_fragment(fragment=None, **kwargs):
    """Filter allowed dataset fragment."""
    file_name = fragment if isinstance(fragment, str) else fragment.path
    file_name = file_name.split("/")[-1]
    file_parts = re.split(r"[_.-]", file_name)
    file_record_type = file_parts[0]
    file_year = int(file_parts[2])
    file_month = int(file_parts[3])

    is_allowed_fragment = (
        (file_record_type == kwargs.get("record_type"))
        and (file_year == kwargs.get("year"))
        and (file_month in (kwargs.get("months") or [kwargs.get("month")] or []))
    )
    return is_allowed_fragment


class ArrowHTTPFileSystem(HTTPFileSystem):
    """Simple File-System for fetching data via HTTP(S)."""

    def __init__(self, **kwargs):
        super().__init__(self, **kwargs)

    async def _info(self, url, **kwargs):
        """Get info of URL."""

        # https://github.com/fsspec/filesystem_spec/blob/master/fsspec/implementations/http.py#L406
        logger = logging.getLogger("fsspec.http")
        info = {}
        session = await self.set_session()

        for policy in ["head", "get"]:
            try:
                info.update(
                    await self._file_info(
                        self.encode_url(url),
                        size_policy=policy,
                        session=session,
                        **self.kwargs,
                        **kwargs,
                    )
                )
                if info.get("size") is not None:
                    break
            except Exception as exc:
                if policy == "get":
                    # If get failed, then raise a FileNotFoundError
                    raise FileNotFoundError(url) from exc
                logger.debug("", exc_info=exc)

        return {"name": url, "size": None, **info, "type": "file"}

    async def _file_info(self, url, session, size_policy="head", **kwargs):
        """Call HEAD on the server to get details about the file (size/checksum etc.)"""

        # https://github.com/fsspec/filesystem_spec/blob/master/fsspec/implementations/http.py#L815
        kwargs = kwargs.copy()
        ar = kwargs.pop("allow_redirects", True)
        head = kwargs.get("headers", {}).copy()
        head["Accept-Encoding"] = "identity"
        kwargs["headers"] = head

        info = {}
        if size_policy == "head":
            r = await session.head(url, allow_redirects=ar, **kwargs)
        elif size_policy == "get":
            r = await session.get(url, allow_redirects=ar, **kwargs)
        else:
            raise TypeError(f'size_policy must be "head" or "get", got {size_policy}')
        async with r:
            r.raise_for_status()

            # TODO:
            #  recognise lack of 'Accept-Ranges',
            #                 or 'Accept-Ranges': 'none' (not 'bytes')
            #  to mean streaming only, no random access => return None
            if "Content-Length" in r.headers:
                # Some servers may choose to ignore Accept-Encoding and return
                # compressed content, in which case the returned size is unreliable.
                if "Content-Encoding" not in r.headers or r.headers[
                    "Content-Encoding"
                ] in [
                    "identity",
                    "",
                ]:
                    info["size"] = int(r.headers["Content-Length"])
            elif "Content-Range" in r.headers:
                info["size"] = int(r.headers["Content-Range"].split("/")[1])

            if "Content-Type" in r.headers:
                info["mimetype"] = r.headers["Content-Type"].partition(";")[0]

            info["url"] = str(r.url)

            for checksum_field in ["ETag", "Content-MD5", "Digest"]:
                if r.headers.get(checksum_field):
                    info[checksum_field] = r.headers[checksum_field]

            # https://github.com/apache/arrow/blob/main/python/pyarrow/fs.py#L306
            mtime = r.headers.get("Last-Modified") or None
            if mtime:
                mtime = parsedate_to_datetime(mtime)
            info["mtime"] = mtime

        return info
