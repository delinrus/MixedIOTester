from __future__ import annotations

from config import validate_config
from calibration import iter_calibration_rows


def test_calibration_writes_rows(tmp_path):
    cfg = validate_config(
        {
            "target": {"type": "file", "path": str(tmp_path / "a.bin"), "size": "64MiB", "direct": False, "create_if_missing": True},
            "io": {"engine": "threads", "alignment": 4096},
            "test": {"runtime_sec": 1, "warmup_sec": 0, "num_threads": 2, "region_start": 0, "region_size": "32MiB", "random_seed": 1},
            "operations": {
                "RR": {"enabled": True, "share": 0.25, "block_size": "4KiB"},
                "RW": {"enabled": True, "share": 0.25, "block_size": "4KiB"},
                "SR": {"enabled": True, "share": 0.25, "block_size": "4KiB"},
                "SW": {"enabled": True, "share": 0.25, "block_size": "4KiB"},
            },
            "calibration": {"enabled": True, "output_path": str(tmp_path / "cal.csv"), "append": False, "runtime_sec": 1, "block_sizes": ["4KiB"]},
            "output": {"print_summary": False, "save_json": False, "save_csv": False},
        }
    )
    rows = list(iter_calibration_rows(cfg))
    # 4 ops * 1 block size
    assert len(rows) == 4
    # column count matches requested format
    assert all(len(r) == 8 for r in rows)
    # iodepth column equals threads (or override)
    assert all(r[4] == "2" for r in rows)

