from __future__ import annotations

import os
import signal
import threading
import time

from model import WorkloadConfig
from scheduler import FixedMixScheduler
from stats import StatsCollector
from io_backend import HAS_PREAD, HAS_PWRITE
from worker import PhaseRef, WorkerContext, worker_loop


class Runner:
    def __init__(self, cfg: WorkloadConfig):
        self.cfg = cfg
        self.scheduler = FixedMixScheduler(cfg)
        self.stats = StatsCollector(cfg)
        self._stop_event = threading.Event()
        self._phase = PhaseRef("warmup")
        self._fd: int | None = None
        self._worker_fds: list[int] = []

    def run(self) -> dict:
        prev_handler = signal.signal(signal.SIGINT, self._sigint_handler)
        started = time.perf_counter()
        interrupted = False
        measured_started = 0.0
        try:
            self._open_target()
            threads = self._start_workers()

            if self.cfg.test.warmup_sec > 0:
                time.sleep(self.cfg.test.warmup_sec)

            self.stats.reset_measured()
            self._phase.set("measured")
            measured_started = time.perf_counter()

            time.sleep(self.cfg.test.runtime_sec)
            self._stop_event.set()

            for t in threads:
                t.join()
        except KeyboardInterrupt:
            interrupted = True
            self._stop_event.set()
        finally:
            signal.signal(signal.SIGINT, prev_handler)
            self._close_target()

        measured_elapsed = (
            (time.perf_counter() - measured_started) if measured_started > 0 else max(0.0, time.perf_counter() - started)
        )
        self.stats.set_runtime(measured_elapsed, interrupted or self._stop_event.is_set())
        return self.stats.build_summary()

    def _start_workers(self) -> list[threading.Thread]:
        if self._fd is None:
            raise RuntimeError("Target is not opened")
        ctx = WorkerContext(
            cfg=self.cfg,
            scheduler=self.scheduler,
            stats=self.stats,
            stop_event=self._stop_event,
            phase_ref=self._phase,
        )
        threads: list[threading.Thread] = []
        self._worker_fds = []
        use_shared_fd = HAS_PREAD and HAS_PWRITE
        for i in range(self.cfg.test.num_threads):
            fd = self._fd if use_shared_fd else self._open_additional_fd()
            self._worker_fds.append(fd)
            t = threading.Thread(target=worker_loop, args=(ctx, i, fd), name=f"worker-{i}", daemon=True)
            t.start()
            threads.append(t)
        return threads

    def _open_target(self) -> None:
        flags = os.O_RDWR
        if self.cfg.target.type == "file":
            if self.cfg.target.create_if_missing:
                flags |= os.O_CREAT
            if self.cfg.target.direct and hasattr(os, "O_DIRECT"):
                flags |= os.O_DIRECT
            self._fd = os.open(self.cfg.target.path, flags, 0o644)
            if self.cfg.target.size is not None and self.cfg.target.size > 0:
                os.ftruncate(self._fd, self.cfg.target.size)
            return
        if self.cfg.target.type == "block_device":
            if self.cfg.target.direct and hasattr(os, "O_DIRECT"):
                flags |= os.O_DIRECT
            self._fd = os.open(self.cfg.target.path, flags)
            return
        raise RuntimeError(f"Unsupported target type: {self.cfg.target.type}")

    def _open_additional_fd(self) -> int:
        flags = os.O_RDWR
        if self.cfg.target.type == "file":
            if self.cfg.target.create_if_missing:
                flags |= os.O_CREAT
            if self.cfg.target.direct and hasattr(os, "O_DIRECT"):
                flags |= os.O_DIRECT
            return os.open(self.cfg.target.path, flags, 0o644)
        if self.cfg.target.type == "block_device":
            if self.cfg.target.direct and hasattr(os, "O_DIRECT"):
                flags |= os.O_DIRECT
            return os.open(self.cfg.target.path, flags)
        raise RuntimeError(f"Unsupported target type: {self.cfg.target.type}")

    def _close_target(self) -> None:
        for fd in getattr(self, "_worker_fds", []):
            if self._fd is not None and fd == self._fd:
                continue
            try:
                os.close(fd)
            except OSError:
                pass
        self._worker_fds = []
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None

    def _sigint_handler(self, signum, frame) -> None:  # noqa: ARG002
        self._stop_event.set()

