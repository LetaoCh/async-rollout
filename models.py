from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class JobStatus(str, Enum):
    PENDING = "PENDING"
    LEASED = "LEASED"
    COMPLETED = "COMPLETED"


@dataclass
class Job:
    job_id: str
    status: JobStatus
    policy_version: int
    assigned_worker: Optional[str] = None
    attempt: int = 0
    result: Optional[dict] = None