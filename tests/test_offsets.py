from __future__ import annotations

from config import validate_config
from scheduler import FixedMixScheduler


def test_random_offsets_within_region():
    raw = {
        "target": {"type": "file", "path": "/tmp/a.bin", "size": "1GiB", "direct": False, "create_if_missing": True},
        "io": {"engine": "threads", "alignment": 4096},
        "test": {
            "runtime_sec": 1,
            "warmup_sec": 0,
            "num_threads": 1,
            "region_start": 4096,
            "region_size": "4MiB",
            "random_seed": 42,
        },
        "operations": {
            "RR": {"enabled": True, "share": 1.0, "block_size": "8KiB"},
            "RW": {"enabled": False, "share": 0.0, "block_size": "4KiB"},
            "SR": {"enabled": False, "share": 0.0, "block_size": "4KiB"},
            "SW": {"enabled": False, "share": 0.0, "block_size": "4KiB"},
        },
    }
    cfg = validate_config(raw)
    sch = FixedMixScheduler(cfg)
    for _ in range(500):
        req = sch.next_request()
        assert req.offset % cfg.io.alignment == 0
        assert cfg.test.region_start <= req.offset
        assert req.offset + req.block_size <= cfg.test.region_start + cfg.test.region_size

