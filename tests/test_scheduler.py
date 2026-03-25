from __future__ import annotations

from config import validate_config
from model import OperationType
from scheduler import FixedMixScheduler


def _cfg():
    raw = {
        "target": {"type": "file", "path": "/tmp/a.bin", "size": "1GiB", "direct": False, "create_if_missing": True},
        "io": {"engine": "threads", "alignment": 4096},
        "test": {
            "runtime_sec": 1,
            "warmup_sec": 0,
            "num_threads": 1,
            "region_start": 0,
            "region_size": "64MiB",
            "random_seed": 1,
        },
        "operations": {
            "RR": {"enabled": True, "share": 0.7, "block_size": "4KiB"},
            "RW": {"enabled": True, "share": 0.1, "block_size": "4KiB"},
            "SR": {"enabled": True, "share": 0.1, "block_size": "4KiB"},
            "SW": {"enabled": True, "share": 0.1, "block_size": "4KiB"},
        },
        "output": {"print_summary": False, "save_json": False, "save_csv": False},
    }
    return validate_config(raw)


def test_scheduler_mix_converges():
    cfg = _cfg()
    sch = FixedMixScheduler(cfg)
    n = 20000
    for _ in range(n):
        sch.next_request()
    issued = sch.issued_counts()
    for op in OperationType:
        expected = cfg.operations[op].share
        actual = issued.get(op, 0) / n
        assert abs(actual - expected) < 0.01


def test_sequential_offsets_wrap():
    cfg = _cfg()
    sch = FixedMixScheduler(cfg)
    sr_offsets = []
    for _ in range(200):
        req = sch.next_request()
        if req.op == OperationType.SR:
            sr_offsets.append(req.offset)
    assert sr_offsets
    assert min(sr_offsets) >= cfg.test.region_start
    assert max(sr_offsets) + cfg.operations[OperationType.SR].block_size <= cfg.test.region_start + cfg.test.region_size

