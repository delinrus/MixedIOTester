from __future__ import annotations

import csv
from dataclasses import replace
from pathlib import Path
from typing import Iterable

from model import OperationConfig, OperationType, WorkloadConfig
from runner import Runner

try:
    import matplotlib.pyplot as plt
except Exception as exc:  # pragma: no cover - runtime dependency guard
    raise RuntimeError(
        "matplotlib is required for --mix-plots. Install dependencies again to continue."
    ) from exc


BLOCK_SIZES_BYTES = [
    4 * 1024,
    8 * 1024,
    16 * 1024,
    32 * 1024,
    64 * 1024,
    128 * 1024,
    256 * 1024,
    1024 * 1024,
]


def _size_to_kib(num_bytes: int) -> int:
    return num_bytes // 1024


def _case_name(shares: dict[OperationType, float]) -> str:
    parts: list[str] = []
    for op in OperationType:
        share = shares.get(op, 0.0)
        if share <= 0:
            continue
        parts.append(f"{op.value}{int(round(share * 100))}")
    return "_".join(parts).lower()


def _build_profiles() -> list[tuple[str, dict[OperationType, float]]]:
    rr = OperationType.RR
    rw = OperationType.RW
    sr = OperationType.SR
    sw = OperationType.SW

    # 6 pairwise combinations: 50/50 each.
    pairwise = [
        {rr: 0.5, rw: 0.5},
        {rr: 0.5, sr: 0.5},
        {rr: 0.5, sw: 0.5},
        {rw: 0.5, sr: 0.5},
        {rw: 0.5, sw: 0.5},
        {sr: 0.5, sw: 0.5},
    ]
    all_equal = [{rr: 0.25, rw: 0.25, sr: 0.25, sw: 0.25}]
    read_heavy = [{rr: 0.4, sr: 0.4, rw: 0.1, sw: 0.1}]
    write_heavy = [{rr: 0.1, sr: 0.1, rw: 0.4, sw: 0.4}]

    all_profiles = pairwise + all_equal + read_heavy + write_heavy
    out: list[tuple[str, dict[OperationType, float]]] = []
    for shares in all_profiles:
        out.append((_case_name(shares), shares))
    return out


def _mix_config(base: WorkloadConfig, shares: dict[OperationType, float], bs_bytes: int) -> WorkloadConfig:
    ops: dict[OperationType, OperationConfig] = {}
    for op, op_cfg in base.operations.items():
        share = shares.get(op, 0.0)
        enabled = share > 0
        block_size = bs_bytes if enabled else op_cfg.block_size
        ops[op] = OperationConfig(
            enabled=enabled,
            share=share,
            block_size=block_size,
            alignment=op_cfg.alignment,
            region_start=op_cfg.region_start,
            region_size=op_cfg.region_size,
        )
    return replace(base, operations=ops)


def _iter_profile_iops(
    base: WorkloadConfig,
    shares: dict[OperationType, float],
    block_sizes: Iterable[int],
) -> list[tuple[int, float]]:
    points: list[tuple[int, float]] = []
    for bs in block_sizes:
        cfg = _mix_config(base=base, shares=shares, bs_bytes=bs)
        summary = Runner(cfg).run()
        iops = float(summary["total"]["iops"])
        points.append((_size_to_kib(bs), iops))
    return points


def _shares_title(shares: dict[OperationType, float]) -> str:
    parts: list[str] = []
    for op in OperationType:
        share = shares.get(op, 0.0)
        if share > 0:
            parts.append(f"{op.value} {int(round(share * 100))}%")
    return ", ".join(parts)


def _save_plot(points: list[tuple[int, float]], title: str, path: Path) -> None:
    x = [p[0] for p in points]
    y = [p[1] for p in points]
    plt.figure(figsize=(10, 6))
    plt.plot(x, y, marker="o", linewidth=2)
    plt.title(title)
    plt.xlabel("Block size (KiB)")
    plt.ylabel("IOPS")
    plt.xticks(x, [str(v) for v in x])
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def _write_points_csv(rows: list[list[str]], path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(
            [
                "profile",
                "rr_share_pct",
                "sr_share_pct",
                "rw_share_pct",
                "sw_share_pct",
                "block_size_kib",
                "iops",
            ]
        )
        writer.writerows(rows)


def generate_mix_plots(base_cfg: WorkloadConfig, output_dir: str) -> None:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    profiles = _build_profiles()
    total = len(profiles)
    csv_rows: list[list[str]] = []
    for idx, (name, shares) in enumerate(profiles, start=1):
        print(f"[mix-plots {idx}/{total}] running profile={name}", flush=True)
        points = _iter_profile_iops(base_cfg, shares, BLOCK_SIZES_BYTES)
        plot_path = out_dir / f"{name}.png"
        _save_plot(points, _shares_title(shares), plot_path)
        for block_kib, iops in points:
            csv_rows.append(
                [
                    name,
                    f"{shares.get(OperationType.RR, 0.0) * 100:.0f}",
                    f"{shares.get(OperationType.SR, 0.0) * 100:.0f}",
                    f"{shares.get(OperationType.RW, 0.0) * 100:.0f}",
                    f"{shares.get(OperationType.SW, 0.0) * 100:.0f}",
                    str(block_kib),
                    f"{iops:.6f}",
                ]
            )
        print(f"[mix-plots {idx}/{total}] saved={plot_path}", flush=True)
    csv_path = out_dir / "mix_iops_vs_block_size.csv"
    _write_points_csv(csv_rows, csv_path)
    print(f"[mix-plots] saved={csv_path}", flush=True)
