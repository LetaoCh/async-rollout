from __future__ import annotations

import time
from typing import Optional

import ray

from models import JobRecord, JobStatus, WorkerRecord, WorkerStatus, RolloutResult


@ray.remote
class Reconciler:
    def __init__(
        self,
        lease_seconds: float = 10.0,
        heartbeat_timeout_seconds: float = 5.0,
        max_retries: int = 2,
    ) -> None:
        self.lease_seconds = lease_seconds
        self.heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self.max_retries = max_retries
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
        print(f"[reconciler] register_worker worker_id={worker_id}")

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
                print(
                    f"[reconciler] lease_job worker_id={worker_id} "
                    f"job_id={job.job_id} attempt={job.attempt} policy_version={job.policy_version}"
                )
                return job

        return None

    def submit_result(self, result: RolloutResult) -> bool:
        job = self.jobs.get(result.job_id)
        if job is None:
            print(
                f"[reconciler] submit_result rejected: unknown job_id={result.job_id} "
                f"worker_id={result.worker_id} attempt={result.attempt}"
            )
            return False

        if job.status != JobStatus.LEASED:
            print(
                f"[reconciler] submit_result rejected: job_id={result.job_id} "
                f"status={job.status.value} (expected LEASED) worker_id={result.worker_id}"
            )
            return False

        if job.attempt != result.attempt:
            print(
                f"[reconciler] submit_result rejected: job_id={result.job_id} "
                f"attempt mismatch job={job.attempt} result={result.attempt} worker_id={result.worker_id}"
            )
            return False

        if job.assigned_worker != result.worker_id:
            print(
                f"[reconciler] submit_result rejected: job_id={result.job_id} "
                f"worker mismatch assigned={job.assigned_worker} result={result.worker_id}"
            )
            return False

        job.status = JobStatus.COMPLETED
        job.result = result.payload
        print(
            f"[reconciler] job completed job_id={result.job_id} "
            f"worker_id={result.worker_id} attempt={result.attempt}"
        )
        return True

    def tick(self) -> None:
        now = time.time()

        # 1. Mark dead workers
        for worker in self.workers.values():
            if now - worker.last_heartbeat > self.heartbeat_timeout_seconds:
                if worker.status != WorkerStatus.DEAD:
                    worker.status = WorkerStatus.DEAD
                    print(f"[reconciler] worker marked DEAD worker_id={worker.worker_id}")

        # 2. Expire leased jobs
        for job in self.jobs.values():
            if job.status != JobStatus.LEASED:
                continue

            if job.lease_expiry is None:
                continue

            if now <= job.lease_expiry:
                continue

            if job.num_retries < self.max_retries:
                job.status = JobStatus.RETRY_PENDING
                job.num_retries += 1
                print(
                    f"[reconciler] lease expired -> retry_pending "
                    f"job_id={job.job_id} assigned_worker={job.assigned_worker} "
                    f"attempt={job.attempt} num_retries={job.num_retries}"
                )
            else:
                job.status = JobStatus.FAILED_PERMANENT
                print(
                    f"[reconciler] lease expired -> failed_permanent "
                    f"job_id={job.job_id} assigned_worker={job.assigned_worker} "
                    f"attempt={job.attempt} num_retries={job.num_retries}"
                )

            job.lease_expiry = None

    def get_summary(self) -> dict:
        counts = {status.value: 0 for status in JobStatus}
        for job in self.jobs.values():
            counts[job.status.value] += 1

        return {
            "num_workers": len(self.workers),
            "job_counts": counts,
        }

    def get_job_table(self) -> list[dict]:
        return [
            {
                "job_id": job.job_id,
                "status": job.status.value,
                "assigned_worker": job.assigned_worker,
                "attempt": job.attempt,
                "policy_version": job.policy_version,
            }
            for job in self.jobs.values()
        ]

    def all_jobs_finished(self) -> bool:
        terminal = {JobStatus.COMPLETED, JobStatus.FAILED_PERMANENT}
        return all(job.status in terminal for job in self.jobs.values())
