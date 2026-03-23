from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict

from model import CompletionRecord, OpStats, OperationType, RunStats, WorkloadConfig


def percentile_ns(values: list[int], p: float) -> int:
    if not values:
        return 0
    seq = sorted(values)
    idx = int((len(seq) - 1) * p)
    return seq[idx]


class StatsCollector:
    def __init__(self, cfg: WorkloadConfig):
        self.cfg = cfg
        self.run = RunStats(per_op={op: OpStats() for op in OperationType})

    def mark_issued(self, op: OperationType) -> None:
        self.run.per_op[op].issued_ops += 1

    def mark_completion(self, rec: CompletionRecord) -> None:
        op_stats = self.run.per_op[rec.metadata.op]
        op_stats.completed_ops += 1
        if rec.result < 0:
            op_stats.errors += 1
            return
        op_stats.bytes += rec.result
        latency = rec.completion_ts_ns - rec.metadata.submit_ts_ns
        if rec.metadata.phase == "measured":
            op_stats.latency_ns.append(latency)

    def set_runtime(self, seconds: float, interrupted: bool) -> None:
        self.run.runtime_sec = seconds
        self.run.interrupted = interrupted

    def build_summary(self) -> Dict[str, Any]:
        runtime = self.run.runtime_sec if self.run.runtime_sec > 0 else 1e-9
        per_op = {}
        totals = {
            "issued_ops": 0,
            "completed_ops": 0,
            "bytes": 0,
            "errors": 0,
            "latency_ns": [],
        }
        for op, st in self.run.per_op.items():
            avg = int(sum(st.latency_ns) / len(st.latency_ns)) if st.latency_ns else 0
            p50 = percentile_ns(st.latency_ns, 0.50)
            p95 = percentile_ns(st.latency_ns, 0.95)
            p99 = percentile_ns(st.latency_ns, 0.99)
            item = {
                "issued_ops": st.issued_ops,
                "completed_ops": st.completed_ops,
                "bytes": st.bytes,
                "errors": st.errors,
                "iops": st.completed_ops / runtime,
                "bandwidth_Bps": st.bytes / runtime,
                "avg_latency_ns": avg,
                "min_latency_ns": min(st.latency_ns) if st.latency_ns else 0,
                "max_latency_ns": max(st.latency_ns) if st.latency_ns else 0,
                "p50_latency_ns": p50,
                "p95_latency_ns": p95,
                "p99_latency_ns": p99,
            }
            per_op[op.value] = item
            totals["issued_ops"] += st.issued_ops
            totals["completed_ops"] += st.completed_ops
            totals["bytes"] += st.bytes
            totals["errors"] += st.errors
            totals["latency_ns"].extend(st.latency_ns)

        all_lat = totals["latency_ns"]
        total_summary = {
            "issued_ops": totals["issued_ops"],
            "completed_ops": totals["completed_ops"],
            "bytes": totals["bytes"],
            "errors": totals["errors"],
            "iops": totals["completed_ops"] / runtime,
            "bandwidth_Bps": totals["bytes"] / runtime,
            "avg_latency_ns": int(sum(all_lat) / len(all_lat)) if all_lat else 0,
            "p50_latency_ns": percentile_ns(all_lat, 0.50),
            "p95_latency_ns": percentile_ns(all_lat, 0.95),
            "p99_latency_ns": percentile_ns(all_lat, 0.99),
        }

        issued_mix = {
            op.value: (self.run.per_op[op].issued_ops / totals["issued_ops"] if totals["issued_ops"] else 0.0)
            for op in OperationType
        }
        completed_mix = {
            op.value: (
                self.run.per_op[op].completed_ops / totals["completed_ops"] if totals["completed_ops"] else 0.0
            )
            for op in OperationType
        }
        return {
            "runtime_sec": self.run.runtime_sec,
            "interrupted": self.run.interrupted,
            "per_op": per_op,
            "total": total_summary,
            "achieved_mix_issued": issued_mix,
            "achieved_mix_completed": completed_mix,
        }

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self.run)

