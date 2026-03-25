from __future__ import annotations

import random
import threading
from dataclasses import dataclass
from typing import Dict

from model import OperationConfig, OperationType, ScheduledRequest, WorkloadConfig


@dataclass
class OpRuntimeState:
    issued: int = 0
    seq_offset: int = 0


class FixedMixScheduler:
    def __init__(self, cfg: WorkloadConfig):
        self.cfg = cfg
        self._rng = random.Random(cfg.test.random_seed)
        self._lock = threading.Lock()
        self._states: Dict[OperationType, OpRuntimeState] = {}
        self._active_ops = []
        for op, op_cfg in cfg.operations.items():
            if op_cfg.enabled and op_cfg.share > 0:
                self._active_ops.append(op)
                start = self._op_region_start(op_cfg)
                self._states[op] = OpRuntimeState(issued=0, seq_offset=start)
        self._total_issued = 0
        self._request_id = 0

    def next_request(self) -> ScheduledRequest:
        with self._lock:
            op = self._pick_op()
            op_cfg = self.cfg.operations[op]
            offset = self._next_offset(op, op_cfg)
            req = ScheduledRequest(
                request_id=self._request_id,
                op=op,
                offset=offset,
                block_size=op_cfg.block_size,
            )
            self._request_id += 1
            self._states[op].issued += 1
            self._total_issued += 1
            return req

    def issued_counts(self) -> Dict[OperationType, int]:
        with self._lock:
            return {op: state.issued for op, state in self._states.items()}

    def total_issued(self) -> int:
        with self._lock:
            return self._total_issued

    def _pick_op(self) -> OperationType:
        best_op = self._active_ops[0]
        best_deficit = float("-inf")
        total = self._total_issued
        for op in self._active_ops:
            target_share = self.cfg.operations[op].share
            expected = total * target_share
            deficit = expected - self._states[op].issued
            if deficit > best_deficit:
                best_deficit = deficit
                best_op = op
        return best_op

    def _next_offset(self, op: OperationType, op_cfg: OperationConfig) -> int:
        start = self._op_region_start(op_cfg)
        size = self._op_region_size(op_cfg)
        end = start + size
        align = op_cfg.alignment or self.cfg.io.alignment

        if op.is_sequential:
            cur = self._states[op].seq_offset
            if cur + op_cfg.block_size > end:
                cur = start
            nxt = cur + op_cfg.block_size
            if nxt + op_cfg.block_size > end:
                nxt = start
            self._states[op].seq_offset = nxt
            return cur

        max_off = end - op_cfg.block_size
        span = max_off - start
        slots = (span // align) + 1
        choice = self._rng.randrange(slots)
        return start + choice * align

    def _op_region_start(self, op_cfg: OperationConfig) -> int:
        return op_cfg.region_start if op_cfg.region_start is not None else self.cfg.test.region_start

    def _op_region_size(self, op_cfg: OperationConfig) -> int:
        return op_cfg.region_size if op_cfg.region_size is not None else self.cfg.test.region_size

