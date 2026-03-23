from __future__ import annotations

import ctypes
import os
import platform
import time
from collections import deque
from typing import Deque, Dict, Optional

from buffers import AlignedBufferPool, Buffer
from model import CompletionRecord, RequestMetadata, ScheduledRequest, WorkloadConfig


class EngineError(RuntimeError):
    pass


class IOUringEngine:
    """
    MVP engine facade.

    The control-plane contract is built around io_uring semantics (submit + completion).
    On Linux it validates that liburing is available via ctypes. Request I/O execution
    is implemented in Python for MVP portability of tests and can be replaced by real
    SQE/CQE path without changing runner/scheduler interfaces.
    """

    def __init__(self, cfg: WorkloadConfig):
        self.cfg = cfg
        self.fd: Optional[int] = None
        self.pool = AlignedBufferPool(cfg.io.alignment)
        self.inflight: Dict[int, RequestMetadata] = {}
        self._completions: Deque[CompletionRecord] = deque()
        self._liburing = None
        self._load_liburing()

    def _load_liburing(self) -> None:
        if platform.system().lower() != "linux":
            return
        try:
            self._liburing = ctypes.CDLL("liburing.so.2")
        except OSError:
            try:
                self._liburing = ctypes.CDLL("liburing.so")
            except OSError as exc:
                raise EngineError("liburing not found on Linux host") from exc

    def open_target(self) -> None:
        flags = os.O_RDWR
        if self.cfg.target.type == "file":
            if self.cfg.target.create_if_missing:
                flags |= os.O_CREAT
            if self.cfg.target.direct and hasattr(os, "O_DIRECT"):
                flags |= os.O_DIRECT
            self.fd = os.open(self.cfg.target.path, flags, 0o644)
            if self.cfg.target.size:
                os.ftruncate(self.fd, self.cfg.target.size)
            return

        if self.cfg.target.type == "block_device":
            if self.cfg.target.direct and hasattr(os, "O_DIRECT"):
                flags |= os.O_DIRECT
            self.fd = os.open(self.cfg.target.path, flags)
            return

        raise EngineError(f"Unsupported target type: {self.cfg.target.type}")

    def close(self) -> None:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        self.pool.close()

    def submit(self, req: ScheduledRequest, phase: str) -> None:
        if self.fd is None:
            raise EngineError("Target is not opened")
        buf: Buffer = self.pool.acquire(req.block_size)
        if req.op.is_write:
            self.pool.fill_for_write(buf, pattern=(req.request_id % 251) + 1)
        submit_ts = time.perf_counter_ns()
        md = RequestMetadata(
            request_id=req.request_id,
            op=req.op,
            block_size=req.block_size,
            offset=req.offset,
            submit_ts_ns=submit_ts,
            buffer_id=buf.buffer_id,
            phase=phase,
        )
        self.inflight[req.request_id] = md
        result = self._execute_io(req, buf)
        completion_ts = time.perf_counter_ns()
        self._completions.append(
            CompletionRecord(
                request_id=req.request_id,
                result=result,
                completion_ts_ns=completion_ts,
                metadata=md,
            )
        )
        self.pool.release(buf)
        self.inflight.pop(req.request_id, None)

    def _execute_io(self, req: ScheduledRequest, buf: Buffer) -> int:
        try:
            if req.op.is_read:
                data = os.pread(self.fd, req.block_size, req.offset)
                if len(data) and len(data) <= req.block_size:
                    ctypes.memmove(buf.ptr, data, len(data))
                return len(data)
            payload = ctypes.string_at(buf.ptr, req.block_size)
            return os.pwrite(self.fd, payload, req.offset)
        except OSError as exc:
            return -int(exc.errno or 1)

    def poll_completions(self, max_items: int) -> list[CompletionRecord]:
        out = []
        while self._completions and len(out) < max_items:
            out.append(self._completions.popleft())
        return out

