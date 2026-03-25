from __future__ import annotations

import os
from typing import Final

HAS_PREAD: Final[bool] = hasattr(os, "pread")
HAS_PWRITE: Final[bool] = hasattr(os, "pwrite")


def pread(fd: int, size: int, offset: int) -> bytes:
    """
    Prefer os.pread when available.

    On platforms without os.pread (notably Windows), fall back to seek+read.
    This is only safe when the fd is NOT shared across threads.
    """
    if HAS_PREAD:
        return os.pread(fd, size, offset)  # type: ignore[attr-defined]
    os.lseek(fd, offset, os.SEEK_SET)
    return os.read(fd, size)


def pwrite(fd: int, data: bytes, offset: int) -> int:
    """
    Prefer os.pwrite when available.

    On platforms without os.pwrite (notably Windows), fall back to seek+write.
    This is only safe when the fd is NOT shared across threads.
    """
    if HAS_PWRITE:
        return os.pwrite(fd, data, offset)  # type: ignore[attr-defined]
    os.lseek(fd, offset, os.SEEK_SET)
    return os.write(fd, data)

