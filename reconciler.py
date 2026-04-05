from __future__ import annotations

import time
from typing import Optional

import ray

from models import JobRecord, JobStatus, WorkerRecord, WorkerStatus, RolloutResult


@ray.remote
class Reconciler:
    def __init__(self, lease_seconds: float = 10.0) -> None:
        self.lease_seconds = lease_seconds
        self.jobs: dict[str, JobRecord] = {}
        self.workers: dict[str, WorkerRecord] = {}

    def seed_jobs(self, num_jobs: int, policy_version: int = 0) -> None:
        for i in range(num_jobs):
            job_id = f"job-{i}"
            self.jobs[job_id] = JobRecord(job_id=job_id, status=JobStatus.PENDING, policy_version=policy_version)

    def register_worker(self, worker_id: str) -> None:
        self.workers[worker_id] = WorkerRecord(
            worker_id=worker_id, status=WorkerStatus.ALIVE, last_heartbeat=time.time()
        )

    def heartbeat(self, worker_id: str) -> None:
        if worker_id not in self.workers:
            self.register_worker(worker_id)
            return

        worker = self.workers[worker_id]
        worker.last_heartbeat = time.time()
        worker.status = WorkerStatus.ALIVE

    def lease_job(self, worker_id: str) -> Optional[JobRecord]:
        now = time.time()

        for job in self.jobs.values():
            if job.status in (JobStatus.PENDING, JobStatus.RETRY_PENDING):
                job.status = JobStatus.LEASED
                job.assigned_worker = worker_id
                job.attempt += 1
                job.lease_expiry = now + self.lease_seconds
                return job

        return None

    def submit_result(self, result: RolloutResult) -> bool:
        job = self.jobs.get(result.job_id)
        if job is None:
            return False

        if job.status != JobStatus.LEASED:
            return False

        if job.attempt != result.attempt:
            return False

        if job.assigned_worker != result.worker_id:
            return False

        job.status = JobStatus.COMPLETED
        job.result = result.payload
        return True

    def get_summary(self) -> dict:
        counts = {status.value: 0 for status in JobStatus}
        for job in self.jobs.values():
            counts[job.status.value] += 1

        return {
            "num_workers": len(self.workers),
            "job_counts": counts,
        }

    def all_jobs_completed(self) -> bool:
        return all(job.status == JobStatus.COMPLETED for job in self.jobs.values())
