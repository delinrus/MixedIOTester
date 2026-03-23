from __future__ import annotations

import signal
import time
from typing import Optional

from model import WorkloadConfig
from scheduler import FixedMixScheduler
from stats import StatsCollector
from uring_engine import IOUringEngine


class Runner:
    def __init__(self, cfg: WorkloadConfig, engine: Optional[IOUringEngine] = None):
        self.cfg = cfg
        self.scheduler = FixedMixScheduler(cfg)
        self.stats = StatsCollector(cfg)
        self.engine = engine or IOUringEngine(cfg)
        self._stop = False

    def run(self) -> dict:
        prev_handler = signal.signal(signal.SIGINT, self._sigint_handler)
        started = time.perf_counter()
        interrupted = False
        try:
            self.engine.open_target()
            self._phase_loop(duration_sec=self.cfg.test.warmup_sec, phase="warmup")
            self._phase_loop(duration_sec=self.cfg.test.runtime_sec, phase="measured")
            self._drain()
        except KeyboardInterrupt:
            interrupted = True
        finally:
            signal.signal(signal.SIGINT, prev_handler)
            self.engine.close()
        elapsed = time.perf_counter() - started
        self.stats.set_runtime(elapsed, interrupted or self._stop)
        return self.stats.build_summary()

    def _phase_loop(self, duration_sec: int, phase: str) -> None:
        if duration_sec <= 0:
            return
        end_ts = time.perf_counter() + duration_sec
        while time.perf_counter() < end_ts and not self._stop:
            self._fill_queue(phase=phase)
            self._poll_once()

    def _fill_queue(self, phase: str) -> None:
        outstanding = len(self.engine.inflight)
        to_submit = self.cfg.io.queue_depth - outstanding
        for _ in range(max(0, to_submit)):
            req = self.scheduler.next_request()
            self.stats.mark_issued(req.op)
            self.engine.submit(req, phase=phase)

    def _poll_once(self) -> None:
        completions = self.engine.poll_completions(max_items=self.cfg.io.queue_depth * 2)
        for rec in completions:
            self.stats.mark_completion(rec)

    def _drain(self) -> None:
        while self.engine.inflight:
            self._poll_once()

    def _sigint_handler(self, signum, frame) -> None:  # noqa: ARG002
        self._stop = True

