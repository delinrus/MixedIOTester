from __future__ import annotations

import csv
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Iterable

from model import OperationConfig, OperationType, WorkloadConfig
from runner import Runner


def _fmt_bs_short(num_bytes: int) -> str:
    # Match style like 4k, 128k, 1M from the example.
    if num_bytes % (1024 * 1024) == 0:
        return f"{num_bytes // (1024 * 1024)}M"
    if num_bytes % 1024 == 0:
        return f"{num_bytes // 1024}k"
    return str(num_bytes)


def _rw_label(op: OperationType) -> str:
    if op == OperationType.RR:
        return "randread"
    if op == OperationType.RW:
        return "randwrite"
    if op == OperationType.SR:
        return "read"
    if op == OperationType.SW:
        return "write"
    return op.value.lower()


def _default_case_name(op: OperationType, bs_bytes: int) -> str:
    return f"{op.value.lower()}_{_fmt_bs_short(bs_bytes)}"


def _single_op_config(base: WorkloadConfig, op: OperationType, bs_bytes: int, num_threads: int) -> WorkloadConfig:
    ops = {}
    for t, oc in base.operations.items():
        enabled = t == op
        share = 1.0 if t == op else 0.0
        block_size = bs_bytes if t == op else oc.block_size
        ops[t] = OperationConfig(
            enabled=enabled,
            share=share,
            block_size=block_size,
            alignment=oc.alignment,
            region_start=oc.region_start,
            region_size=oc.region_size,
        )
    test = replace(base.test, runtime_sec=base.calibration.runtime_sec, warmup_sec=base.calibration.warmup_sec, num_threads=num_threads)
    return replace(base, test=test, operations=ops)


def iter_calibration_rows(cfg: WorkloadConfig) -> Iterable[list[str]]:
    if not cfg.calibration.block_sizes:
        # If not specified, calibrate using the block_size from config for each enabled op.
        sizes = sorted({oc.block_size for oc in cfg.operations.values() if oc.enabled and oc.share > 0})
    else:
        sizes = cfg.calibration.block_sizes

    for op in OperationType:
        threads = cfg.calibration.num_threads.get(op.value, cfg.test.num_threads)
        for bs in sizes:
            case_cfg = _single_op_config(cfg, op=op, bs_bytes=bs, num_threads=threads)
            name = f"{cfg.calibration.name_prefix}{_default_case_name(op, bs)}"
            summary = Runner(case_cfg).run()
            total = summary["total"]
            ts = datetime.now().isoformat()
            iops = float(total["iops"])
            bw_mib_s = float(total["bandwidth_Bps"]) / (1024.0 * 1024.0)
            yield [
                ts,
                name,
                _rw_label(op),
                _fmt_bs_short(bs),
                str(threads),
                str(case_cfg.test.runtime_sec),
                f"{iops:.0f}",
                f"{bw_mib_s:.3f}",
            ]


def write_calibration_csv(cfg: WorkloadConfig) -> None:
    p = Path(cfg.calibration.output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if cfg.calibration.append else "w"
    write_header = (not p.exists()) or (not cfg.calibration.append)
    with p.open(mode, encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        if write_header:
            w.writerow(["ts", "name", "rw", "bs", "iodepth", "runtime_s", "iops", "bw_mib_s"])
        for row in iter_calibration_rows(cfg):
            w.writerow(row)

