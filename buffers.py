from __future__ import annotations

import ctypes
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict


@dataclass
class Buffer:
    buffer_id: int
    ptr: int
    size: int
    data: ctypes.Array


class AlignedBufferPool:
    def __init__(self, alignment: int):
        self.alignment = alignment
        self._next_id = 0
        self._free: Dict[int, Deque[Buffer]] = defaultdict(deque)
        self._all: Dict[int, Buffer] = {}
        self._libc = ctypes.CDLL("libc.so.6")
        self._libc.posix_memalign.argtypes = [
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.c_size_t,
            ctypes.c_size_t,
        ]
        self._libc.posix_memalign.restype = ctypes.c_int
        self._libc.free.argtypes = [ctypes.c_void_p]
        self._libc.free.restype = None

    def acquire(self, size: int) -> Buffer:
        if self._free[size]:
            return self._free[size].popleft()
        ptr = ctypes.c_void_p()
        rc = self._libc.posix_memalign(ctypes.byref(ptr), self.alignment, size)
        if rc != 0 or not ptr.value:
            raise MemoryError(f"posix_memalign failed rc={rc} size={size}")
        buf_type = ctypes.c_char * size
        data = buf_type.from_address(ptr.value)
        buf = Buffer(buffer_id=self._next_id, ptr=ptr.value, size=size, data=data)
        self._next_id += 1
        self._all[buf.buffer_id] = buf
        return buf

    def release(self, buf: Buffer) -> None:
        self._free[buf.size].append(buf)

    def fill_for_write(self, buf: Buffer, pattern: int = 0x5A) -> None:
        ctypes.memset(buf.ptr, pattern, buf.size)

    def close(self) -> None:
        for buf in self._all.values():
            self._libc.free(ctypes.c_void_p(buf.ptr))
        self._all.clear()
        self._free.clear()

