"""
Re-usable utilities.

"""

import datetime
import logging
from email.utils import parsedate_to_datetime

from fsspec.implementations.http import HTTPFileSystem

__all__ = ["ArrowHTTPFileSystem"]


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
        logger = logging.getLogger("fsspec.http")
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
