from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Any


class JobStatus(str, Enum):
    PENDING = "PENDING"
    LEASED = "LEASED"
    COMPLETED = "COMPLETED"
    RETRY_PENDING = "RETRY_PENDING"
    FAILED_PERMANENT = "FAILED_PERMANENT"


class WorkerStatus(str, Enum):
    ALIVE = "ALIVE"
    DEAD = "DEAD"


@dataclass
class JobRecord:
    job_id: str
    status: JobStatus
    policy_version: int
    assigned_worker: Optional[str] = None
    attempt: int = 0
    lease_expiry: Optional[float] = None
    num_retries: int = 0
    result: Optional[dict[str, Any]] = None


@dataclass
class WorkerRecord:
    worker_id: str
    status: str
    last_heartbeat: float


@dataclass
class RolloutResult:
    job_id: str
    worker_id: str
    attempt: int
    policy_version: int
    payload: dict[str, Any]
