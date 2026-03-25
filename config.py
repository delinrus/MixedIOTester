from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict

import yaml

from model import (
    CalibrationConfig,
    IOConfig,
    OperationConfig,
    OperationType,
    OutputConfig,
    RuntimeConfig,
    TargetConfig,
    TestConfig,
    WorkloadConfig,
)

SIZE_UNITS = {
    "B": 1,
    "KIB": 1024,
    "MIB": 1024**2,
    "GIB": 1024**3,
    "TIB": 1024**4,
    "KB": 1000,
    "MB": 1000**2,
    "GB": 1000**3,
    "TB": 1000**4,
}


class ConfigError(ValueError):
    pass


def parse_size(value: Any) -> int:
    if isinstance(value, int):
        if value < 0:
            raise ConfigError(f"Size must be >= 0, got {value}")
        return value
    if isinstance(value, float):
        if value < 0:
            raise ConfigError(f"Size must be >= 0, got {value}")
        return int(value)
    if not isinstance(value, str):
        raise ConfigError(f"Invalid size value: {value!r}")

    raw = value.strip().upper().replace(" ", "")
    if raw.isdigit():
        return int(raw)

    num = ""
    unit = ""
    for ch in raw:
        if ch.isdigit() or ch == ".":
            num += ch
        else:
            unit += ch
    if not num or not unit:
        raise ConfigError(f"Invalid size format: {value!r}")
    if unit not in SIZE_UNITS:
        raise ConfigError(f"Unsupported size unit: {unit}")
    return int(float(num) * SIZE_UNITS[unit])


def _load_raw(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise ConfigError(f"Config file not found: {path}")
    text = p.read_text(encoding="utf-8")
    suffix = p.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        return yaml.safe_load(text) or {}
    if suffix == ".json":
        return json.loads(text)
    raise ConfigError(f"Unsupported config extension: {p.suffix}")


def _apply_overrides(raw: Dict[str, Any], overrides: list[str]) -> Dict[str, Any]:
    for item in overrides:
        if "=" not in item:
            raise ConfigError(f"Invalid override, expected key=value: {item}")
        key, val = item.split("=", 1)
        parts = key.split(".")
        node = raw
        for part in parts[:-1]:
            if part not in node or not isinstance(node[part], dict):
                node[part] = {}
            node = node[part]
        node[parts[-1]] = _parse_override_value(val)
    return raw


def _parse_override_value(raw: str) -> Any:
    v = raw.strip()
    if v.lower() in {"true", "false"}:
        return v.lower() == "true"
    try:
        if "." in v:
            return float(v)
        return int(v)
    except ValueError:
        return v


def load_config(path: str, overrides: list[str] | None = None) -> WorkloadConfig:
    raw = _load_raw(path)
    if overrides:
        raw = _apply_overrides(raw, overrides)
    return validate_config(raw)


def validate_config(raw: Dict[str, Any]) -> WorkloadConfig:
    try:
        target_raw = raw["target"]
        io_raw = raw.get("io", {})
        test_raw = raw["test"]
        ops_raw = raw["operations"]
    except KeyError as exc:
        raise ConfigError(f"Missing top-level section: {exc}") from exc

    target = TargetConfig(
        type=str(target_raw["type"]),
        path=str(target_raw["path"]),
        size=parse_size(target_raw.get("size", 0)) if target_raw.get("size") is not None else None,
        direct=bool(target_raw.get("direct", False)),
        create_if_missing=bool(target_raw.get("create_if_missing", False)),
    )
    io_cfg = IOConfig(
        engine=str(io_raw.get("engine", "threads")),
        alignment=parse_size(io_raw.get("alignment", 4096)),
    )
    test_cfg = TestConfig(
        runtime_sec=int(test_raw["runtime_sec"]),
        warmup_sec=int(test_raw.get("warmup_sec", 0)),
        num_threads=int(test_raw.get("num_threads", 1)),
        region_start=parse_size(test_raw.get("region_start", 0)),
        region_size=parse_size(test_raw["region_size"]),
        random_seed=test_raw.get("random_seed"),
    )
    output_raw = raw.get("output", {})
    output = OutputConfig(
        print_summary=bool(output_raw.get("print_summary", True)),
        save_json=bool(output_raw.get("save_json", True)),
        save_csv=bool(output_raw.get("save_csv", False)),
        json_path=str(output_raw.get("json_path", "./result.json")),
        csv_path=str(output_raw.get("csv_path", "./result.csv")),
    )
    runtime_raw = raw.get("runtime", {})
    runtime_cfg = RuntimeConfig(
        abort_on_error=bool(runtime_raw.get("abort_on_error", False)),
        debug_logging=bool(runtime_raw.get("debug_logging", False)),
    )
    calibration_raw = raw.get("calibration", {})
    cal_block_sizes = calibration_raw.get("block_sizes", [])
    if cal_block_sizes is None:
        cal_block_sizes = []
    if not isinstance(cal_block_sizes, list):
        raise ConfigError("calibration.block_sizes must be a list")
    calibration_cfg = CalibrationConfig(
        enabled=bool(calibration_raw.get("enabled", False)),
        output_path=str(calibration_raw.get("output_path", "./calibration.csv")),
        append=bool(calibration_raw.get("append", True)),
        runtime_sec=int(calibration_raw.get("runtime_sec", 60)),
        warmup_sec=int(calibration_raw.get("warmup_sec", 0)),
        block_sizes=[parse_size(v) for v in cal_block_sizes],
        num_threads={str(k): int(v) for k, v in (calibration_raw.get("num_threads", {}) or {}).items()},
        name_prefix=str(calibration_raw.get("name_prefix", "")),
    )

    operations: Dict[OperationType, OperationConfig] = {}
    for op in OperationType:
        op_raw = ops_raw.get(op.value, {})
        enabled = bool(op_raw.get("enabled", False))
        share = float(op_raw.get("share", 0.0))
        block_size = parse_size(op_raw.get("block_size", 0)) if enabled or share > 0 else 0
        operations[op] = OperationConfig(
            enabled=enabled,
            share=share,
            block_size=block_size,
            alignment=parse_size(op_raw["alignment"]) if "alignment" in op_raw else None,
            region_start=parse_size(op_raw["region_start"]) if "region_start" in op_raw else None,
            region_size=parse_size(op_raw["region_size"]) if "region_size" in op_raw else None,
        )

    cfg = WorkloadConfig(
        target=target,
        io=io_cfg,
        test=test_cfg,
        operations=operations,
        output=output,
        runtime=runtime_cfg,
        calibration=calibration_cfg,
    )
    _validate_semantics(cfg)
    return cfg


def _validate_semantics(cfg: WorkloadConfig) -> None:
    if cfg.io.engine != "threads":
        raise ConfigError("Only threads engine is supported in MVP")
    if cfg.io.alignment <= 0:
        raise ConfigError("io.alignment must be > 0")
    if cfg.test.runtime_sec <= 0:
        raise ConfigError("test.runtime_sec must be > 0")
    if cfg.test.warmup_sec < 0:
        raise ConfigError("test.warmup_sec must be >= 0")
    if cfg.test.num_threads <= 0:
        raise ConfigError("test.num_threads must be > 0")
    if cfg.test.region_size <= 0:
        raise ConfigError("test.region_size must be > 0")
    if cfg.calibration.enabled:
        if cfg.calibration.runtime_sec <= 0:
            raise ConfigError("calibration.runtime_sec must be > 0")
        if cfg.calibration.warmup_sec < 0:
            raise ConfigError("calibration.warmup_sec must be >= 0")
        for bs in cfg.calibration.block_sizes:
            if bs <= 0:
                raise ConfigError("calibration.block_sizes must contain only positive sizes")

    active = [op for op, op_cfg in cfg.operations.items() if op_cfg.enabled and op_cfg.share > 0]
    if not active:
        raise ConfigError("At least one operation with enabled=true and share>0 is required")
    share_sum = sum(cfg.operations[op].share for op in active)
    if abs(share_sum - 1.0) > 1e-6:
        raise ConfigError(f"Active operation shares must sum to 1.0, got {share_sum}")

    max_bs = 0
    for op, op_cfg in cfg.operations.items():
        if not (op_cfg.enabled and op_cfg.share > 0):
            continue
        if op_cfg.block_size <= 0:
            raise ConfigError(f"{op.value}.block_size must be > 0")
        align = op_cfg.alignment or cfg.io.alignment
        if op_cfg.block_size % align != 0:
            raise ConfigError(f"{op.value}.block_size must be a multiple of alignment={align}")
        max_bs = max(max_bs, op_cfg.block_size)
    if cfg.test.region_size < max_bs:
        raise ConfigError("test.region_size must be >= max active block_size")

    if cfg.target.type not in {"file", "block_device"}:
        raise ConfigError("target.type must be 'file' or 'block_device'")
    if cfg.target.type == "file" and cfg.target.size is not None and cfg.target.size < cfg.test.region_size:
        raise ConfigError("target.size must be >= test.region_size for file targets")
    if cfg.target.direct and cfg.test.region_start % cfg.io.alignment != 0:
        raise ConfigError("test.region_start must be aligned when direct=true")


def config_to_dict(cfg: WorkloadConfig) -> Dict[str, Any]:
    return asdict(cfg)

