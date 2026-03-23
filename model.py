from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional


class OperationType(str, Enum):
    RR = "RR"
    RW = "RW"
    SR = "SR"
    SW = "SW"

    @property
    def is_read(self) -> bool:
        return self in {OperationType.RR, OperationType.SR}

    @property
    def is_write(self) -> bool:
        return self in {OperationType.RW, OperationType.SW}

    @property
    def is_random(self) -> bool:
        return self in {OperationType.RR, OperationType.RW}

    @property
    def is_sequential(self) -> bool:
        return self in {OperationType.SR, OperationType.SW}


@dataclass(frozen=True)
class OperationConfig:
    enabled: bool
    share: float
    block_size: int
    alignment: Optional[int] = None
    region_start: Optional[int] = None
    region_size: Optional[int] = None


@dataclass(frozen=True)
class TargetConfig:
    type: str
    path: str
    size: Optional[int]
    direct: bool
    create_if_missing: bool = False


@dataclass(frozen=True)
class IOConfig:
    engine: str
    queue_depth: int
    alignment: int


@dataclass(frozen=True)
class TestConfig:
    runtime_sec: int
    warmup_sec: int
    region_start: int
    region_size: int
    random_seed: Optional[int] = None


@dataclass(frozen=True)
class OutputConfig:
    print_summary: bool = True
    save_json: bool = True
    save_csv: bool = False
    json_path: str = "./result.json"
    csv_path: str = "./result.csv"


@dataclass(frozen=True)
class WorkloadConfig:
    target: TargetConfig
    io: IOConfig
    test: TestConfig
    operations: Dict[OperationType, OperationConfig]
    output: OutputConfig


@dataclass(frozen=True)
class ScheduledRequest:
    request_id: int
    op: OperationType
    offset: int
    block_size: int


@dataclass(frozen=True)
class RequestMetadata:
    request_id: int
    op: OperationType
    block_size: int
    offset: int
    submit_ts_ns: int
    buffer_id: int
    phase: str  # warmup | measured


@dataclass(frozen=True)
class CompletionRecord:
    request_id: int
    result: int
    completion_ts_ns: int
    metadata: RequestMetadata


@dataclass
class OpStats:
    issued_ops: int = 0
    completed_ops: int = 0
    bytes: int = 0
    errors: int = 0
    latency_ns: list[int] = field(default_factory=list)


@dataclass
class RunStats:
    per_op: Dict[OperationType, OpStats]
    runtime_sec: float = 0.0
    interrupted: bool = False

