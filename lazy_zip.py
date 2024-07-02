# Copyright (c) 2008-present The pip developers (see AUTHORS.txt file)
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

# (Modified async-compatiable `pip._internal.network.lazy_wheel`)

from bisect import bisect_left, bisect_right
from contextlib import contextmanager
from tempfile import NamedTemporaryFile
from typing import List, Optional, Tuple, Generator
from zipfile import ZipFile, BadZipFile

import httpx

CONTENT_CHUNK_SIZE = 8192
HEADERS = {"Accept-Encoding": "identity"}


class LazyZipOverHTTP:
    """File-like object mapped to a ZIP file over HTTP.

    This uses HTTP range requests to lazily fetch the file's content,
    which is supposed to be fed to ZipFile.  If such requests are not
    supported by the server, raise HTTPRangeRequestUnsupported
    during initialization.
    """

    def __init__(
        self, url: str, chunk_size: int = CONTENT_CHUNK_SIZE
    ) -> None:
        session = httpx.Client(follow_redirects=True)
        head = session.head(url, headers=HEADERS)
        head.raise_for_status()
        assert head.status_code == 200
        self._session, self._url, self._chunk_size = session, url, chunk_size
        self._length = int(head.headers["Content-Length"])
        self._file = NamedTemporaryFile()
        self.truncate(self._length)
        self._left: List[int] = []
        self._right: List[int] = []
        self._check_zip()

    @property
    def mode(self) -> str:
        """Opening mode, which is always rb."""
        return "rb"

    @property
    def name(self) -> str:
        """Path to the underlying file."""
        return self._file.name

    def seekable(self) -> bool:
        """Return whether random access is supported, which is True."""
        return True

    def close(self) -> None:
        """Close the file."""
        self._file.close()

    @property
    def closed(self) -> bool:
        """Whether the file is closed."""
        return self._file.closed

    def read(self, size: int = -1) -> bytes:
        """Read up to size bytes from the object and return them.

        As a convenience, if size is unspecified or -1,
        all bytes until EOF are returned.  Fewer than
        size bytes may be returned if EOF is reached.
        """
        download_size = max(size, self._chunk_size)
        start, length = self.tell(), self._length
        stop = length if size < 0 else min(start + download_size, length)
        start = max(0, stop - download_size)
        self._download(start, stop - 1)
        return self._file.read(size)

    def readable(self) -> bool:
        """Return whether the file is readable, which is True."""
        return True

    def seek(self, offset: int, whence: int = 0) -> int:
        """Change stream position and return the new absolute position.

        Seek to offset relative position indicated by whence:
        * 0: Start of stream (the default).  pos should be >= 0;
        * 1: Current position - pos may be negative;
        * 2: End of stream - pos usually negative.
        """
        return self._file.seek(offset, whence)

    def tell(self) -> int:
        """Return the current position."""
        return self._file.tell()

    def truncate(self, size: Optional[int] = None) -> int:
        """Resize the stream to the given size in bytes.

        If size is unspecified resize to the current position.
        The current stream position isn't changed.

        Return the new file size.
        """
        return self._file.truncate(size)

    def writable(self) -> bool:
        """Return False."""
        return False

    def __enter__(self) -> "LazyZipOverHTTP":
        self._file.__enter__()
        return self

    def __exit__(self, *exc) -> None:
        self._file.__exit__(*exc)

    @contextmanager
    def _stay(self) -> Generator[None, None, None]:
        """Return a context manager keeping the position.

        At the end of the block, seek back to original position.
        """
        pos = self.tell()
        try:
            yield
        finally:
            self.seek(pos)

    def _check_zip(self) -> None:
        """Check and download until the file is a valid ZIP."""
        end = self._length - 1
        for start in reversed(range(0, end, self._chunk_size)):
            self._download(start, end)
            with self._stay():
                try:
                    # For read-only ZIP files, ZipFile only needs
                    # methods read, seek, seekable and tell.
                    ZipFile(self)  # type: ignore
                except BadZipFile:
                    pass
                else:
                    break

    def _stream_response(
        self, start: int, end: int, base_headers: dict[str, str] = HEADERS
    ):
        """Return HTTP response to a range request from start to end."""
        headers = base_headers.copy()
        headers["Range"] = f"bytes={start}-{end}"
        # TODO: Get range requests to be correctly cached
        headers["Cache-Control"] = "no-cache"
        return self._session.get(self._url, headers=headers)

    def _merge(
        self, start: int, end: int, left: int, right: int
    ) -> Generator[Tuple[int, int], None, None]:
        """Return a generator of intervals to be fetched.

        Args:
            start (int): Start of needed interval
            end (int): End of needed interval
            left (int): Index of first overlapping downloaded data
            right (int): Index after last overlapping downloaded data
        """
        lslice, rslice = self._left[left:right], self._right[left:right]
        i = start = min([start] + lslice[:1])
        end = max([end] + rslice[-1:])
        for j, k in zip(lslice, rslice):
            if j > i:
                yield i, j - 1
            i = k + 1
        if i <= end:
            yield i, end
        self._left[left:right], self._right[left:right] = [start], [end]

    def _download(self, start: int, end: int) -> None:
        """Download bytes from start to end inclusively."""
        with self._stay():
            left = bisect_left(self._right, start)
            right = bisect_right(self._left, end)
            for start, end in self._merge(start, end, left, right):
                response = self._stream_response(start, end)
                response.raise_for_status()
                self.seek(start)
                for chunk in response.iter_bytes(self._chunk_size):
                    self._file.write(chunk)
