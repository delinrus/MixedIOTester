from __future__ import annotations

import time

from config import validate_config
from model import CompletionRecord, RequestMetadata
from runner import Runner


class FakeEngine:
    def __init__(self, cfg):
        self.cfg = cfg
        self.inflight = {}
        self._completions = []

    def open_target(self):
        return None

    def close(self):
        return None

    def submit(self, req, phase: str):
        submit_ts = time.perf_counter_ns()
        md = RequestMetadata(
            request_id=req.request_id,
            op=req.op,
            block_size=req.block_size,
            offset=req.offset,
            submit_ts_ns=submit_ts,
            buffer_id=0,
            phase=phase,
        )
        self.inflight[req.request_id] = md
        self._completions.append(
            CompletionRecord(
                request_id=req.request_id,
                result=req.block_size,
                completion_ts_ns=submit_ts + 1000,
                metadata=md,
            )
        )
        self.inflight.pop(req.request_id, None)

    def poll_completions(self, max_items: int):
        out = self._completions[:max_items]
        self._completions = self._completions[max_items:]
        return out


def _cfg():
    return validate_config(
        {
            "target": {"type": "file", "path": "/tmp/a.bin", "size": "1GiB", "direct": False, "create_if_missing": True},
            "io": {"engine": "io_uring", "queue_depth": 16, "alignment": 4096},
            "test": {"runtime_sec": 1, "warmup_sec": 0, "region_start": 0, "region_size": "32MiB", "random_seed": 7},
            "operations": {
                "RR": {"enabled": True, "share": 0.5, "block_size": "4KiB"},
                "RW": {"enabled": True, "share": 0.5, "block_size": "4KiB"},
                "SR": {"enabled": False, "share": 0.0, "block_size": "4KiB"},
                "SW": {"enabled": False, "share": 0.0, "block_size": "4KiB"},
            },
            "output": {"print_summary": False, "save_json": False, "save_csv": False},
        }
    )


def test_smoke_short_run():
    cfg = _cfg()
    runner = Runner(cfg, engine=FakeEngine(cfg))
    summary = runner.run()
    assert summary["total"]["completed_ops"] > 0
    assert summary["total"]["bytes"] > 0


def test_mix_proportion_issued_is_close():
    cfg = _cfg()
    runner = Runner(cfg, engine=FakeEngine(cfg))
    summary = runner.run()
    rr = summary["achieved_mix_issued"]["RR"]
    rw = summary["achieved_mix_issued"]["RW"]
    assert abs(rr - 0.5) < 0.05
    assert abs(rw - 0.5) < 0.05

