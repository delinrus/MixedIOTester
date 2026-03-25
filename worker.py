from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional

from io_backend import pwrite, pread
from model import CompletionRecord, OperationType, RequestMetadata, ScheduledRequest, WorkloadConfig
from scheduler import FixedMixScheduler
from stats import StatsCollector


@dataclass(frozen=True)
class WorkerContext:
    cfg: WorkloadConfig
    scheduler: FixedMixScheduler
    stats: StatsCollector
    stop_event: threading.Event
    phase_ref: "PhaseRef"


class PhaseRef:
    def __init__(self, initial: str = "warmup"):
        self._lock = threading.Lock()
        self._phase = initial

    def set(self, phase: str) -> None:
        with self._lock:
            self._phase = phase

    def get(self) -> str:
        with self._lock:
            return self._phase


def _build_write_buffers(cfg: WorkloadConfig) -> Dict[OperationType, bytes]:
    out: Dict[OperationType, bytes] = {}
    for op, op_cfg in cfg.operations.items():
        if not (op_cfg.enabled and op_cfg.share > 0 and op.is_write):
            continue
        pattern = (hash(op.value) & 0xFF) or 0xAB
        out[op] = bytes([pattern]) * op_cfg.block_size
    return out


def worker_loop(
    ctx: WorkerContext,
    worker_id: int,
    fd: int,
    write_buffers: Optional[Dict[OperationType, bytes]] = None,
) -> None:
    abort_on_error = ctx.cfg.runtime.abort_on_error
    debug = ctx.cfg.runtime.debug_logging
    write_bufs = write_buffers or _build_write_buffers(ctx.cfg)

    while not ctx.stop_event.is_set():
        phase = ctx.phase_ref.get()
        req: ScheduledRequest = ctx.scheduler.next_request()
        ctx.stats.mark_issued(req.op, phase=phase)

        submit_ts = time.perf_counter_ns()
        md = RequestMetadata(
            request_id=req.request_id,
            op=req.op,
            block_size=req.block_size,
            offset=req.offset,
            submit_ts_ns=submit_ts,
            buffer_id=worker_id,
            phase=phase,
        )
        try:
            if req.op.is_read:
                data = pread(fd, req.block_size, req.offset)
                res = len(data)
            else:
                payload = write_bufs[req.op]
                res = pwrite(fd, payload, req.offset)
        except OSError as exc:
            res = -int(exc.errno or 1)
            if debug:
                print(f"[worker {worker_id}] io error op={req.op.value} off={req.offset} bs={req.block_size} err={exc!r}")
            if abort_on_error:
                ctx.stop_event.set()

        completion_ts = time.perf_counter_ns()
        ctx.stats.mark_completion(
            CompletionRecord(
                request_id=req.request_id,
                result=res,
                completion_ts_ns=completion_ts,
                metadata=md,
            )
        )

