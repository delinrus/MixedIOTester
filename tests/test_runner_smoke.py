from __future__ import annotations

from config import validate_config
from runner import Runner

def _cfg(path: str):
    return validate_config(
        {
            "target": {"type": "file", "path": path, "size": "64MiB", "direct": False, "create_if_missing": True},
            "io": {"engine": "threads", "alignment": 4096},
            "test": {
                "runtime_sec": 1,
                "warmup_sec": 0,
                "num_threads": 2,
                "region_start": 0,
                "region_size": "32MiB",
                "random_seed": 7,
            },
            "operations": {
                "RR": {"enabled": True, "share": 0.5, "block_size": "4KiB"},
                "RW": {"enabled": True, "share": 0.5, "block_size": "4KiB"},
                "SR": {"enabled": False, "share": 0.0, "block_size": "4KiB"},
                "SW": {"enabled": False, "share": 0.0, "block_size": "4KiB"},
            },
            "output": {"print_summary": False, "save_json": False, "save_csv": False},
        }
    )


def test_smoke_short_run(tmp_path):
    cfg = _cfg(str(tmp_path / "a.bin"))
    runner = Runner(cfg)
    summary = runner.run()
    assert summary["total"]["completed_ops"] > 0
    assert summary["total"]["bytes"] > 0


def test_mix_proportion_issued_is_close(tmp_path):
    cfg = _cfg(str(tmp_path / "a.bin"))
    runner = Runner(cfg)
    summary = runner.run()
    rr = summary["achieved_mix_issued"]["RR"]
    rw = summary["achieved_mix_issued"]["RW"]
    assert abs(rr - 0.5) < 0.05
    assert abs(rw - 0.5) < 0.05

