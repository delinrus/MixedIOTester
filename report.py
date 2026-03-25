from __future__ import annotations

import csv
import json
import platform
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from config import config_to_dict
from model import WorkloadConfig


def _fmt_bw(bps: float) -> str:
    kib = bps / 1024.0
    mib = kib / 1024.0
    return f"{bps:.2f} B/s | {kib:.2f} KiB/s | {mib:.2f} MiB/s"


def print_summary(cfg: WorkloadConfig, summary: Dict[str, Any]) -> None:
    print("=== MixedIOTester Summary ===")
    print(f"runtime_sec={summary['runtime_sec']:.3f} interrupted={summary['interrupted']}")
    print(f"num_threads={cfg.test.num_threads}")
    print("---- target mix ----")
    for op, op_cfg in cfg.operations.items():
        if op_cfg.enabled and op_cfg.share > 0:
            print(f"{op.value}: {op_cfg.share:.4f} block_size={op_cfg.block_size}")
    total = summary["total"]
    print(f"total_iops={total['iops']:.2f}")
    print(f"total_bw={_fmt_bw(total['bandwidth_Bps'])}")
    print(
        "latency_ns p50/p95/p99="
        f"{total['p50_latency_ns']}/{total['p95_latency_ns']}/{total['p99_latency_ns']}"
    )
    print("---- per-op ----")
    for op_name, st in summary["per_op"].items():
        print(
            f"{op_name}: issued={st['issued_ops']} completed={st['completed_ops']} "
            f"iops={st['iops']:.2f} bw={_fmt_bw(st['bandwidth_Bps'])} errors={st['errors']}"
        )
    print("---- achieved mix issued ----")
    for op_name, share in summary["achieved_mix_issued"].items():
        print(f"{op_name}: {share:.4f}")
    print("---- achieved mix completed ----")
    for op_name, share in summary["achieved_mix_completed"].items():
        print(f"{op_name}: {share:.4f}")


def write_json_report(cfg: WorkloadConfig, summary: Dict[str, Any], path: str) -> None:
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "environment": {"platform": platform.platform(), "python": platform.python_version()},
        "config": config_to_dict(cfg),
        "summary": summary,
    }
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_csv_report(summary: Dict[str, Any], path: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "op",
                "issued_ops",
                "completed_ops",
                "bytes",
                "errors",
                "iops",
                "bandwidth_Bps",
                "p50_latency_ns",
                "p95_latency_ns",
                "p99_latency_ns",
            ]
        )
        for op_name, st in summary["per_op"].items():
            writer.writerow(
                [
                    op_name,
                    st["issued_ops"],
                    st["completed_ops"],
                    st["bytes"],
                    st["errors"],
                    st["iops"],
                    st["bandwidth_Bps"],
                    st["p50_latency_ns"],
                    st["p95_latency_ns"],
                    st["p99_latency_ns"],
                ]
            )
        t = summary["total"]
        writer.writerow(
            [
                "TOTAL",
                t["issued_ops"],
                t["completed_ops"],
                t["bytes"],
                t["errors"],
                t["iops"],
                t["bandwidth_Bps"],
                t["p50_latency_ns"],
                t["p95_latency_ns"],
                t["p99_latency_ns"],
            ]
        )

