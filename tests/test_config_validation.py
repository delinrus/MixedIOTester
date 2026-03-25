from __future__ import annotations

import pytest

from config import ConfigError, validate_config


def _base():
    return {
        "target": {"type": "file", "path": "/tmp/a.bin", "size": "1GiB", "direct": False, "create_if_missing": True},
        "io": {"engine": "threads", "alignment": 4096},
        "test": {"runtime_sec": 2, "warmup_sec": 0, "num_threads": 2, "region_start": 0, "region_size": "16MiB"},
        "operations": {
            "RR": {"enabled": True, "share": 1.0, "block_size": "4KiB"},
            "RW": {"enabled": False, "share": 0.0, "block_size": "4KiB"},
            "SR": {"enabled": False, "share": 0.0, "block_size": "4KiB"},
            "SW": {"enabled": False, "share": 0.0, "block_size": "4KiB"},
        },
    }


def test_valid_config():
    cfg = validate_config(_base())
    assert cfg.test.num_threads == 2


def test_shares_sum_must_be_one():
    raw = _base()
    raw["operations"]["RR"]["share"] = 0.5
    with pytest.raises(ConfigError):
        validate_config(raw)


def test_num_threads_must_be_positive():
    raw = _base()
    raw["test"]["num_threads"] = 0
    with pytest.raises(ConfigError):
        validate_config(raw)


def test_block_size_must_align():
    raw = _base()
    raw["operations"]["RR"]["block_size"] = 3000
    with pytest.raises(ConfigError):
        validate_config(raw)

