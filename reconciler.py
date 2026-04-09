from __future__ import annotations

import json
import time
from typing import Any, Optional

import ray

from models import JobRecord, JobStatus, WorkerRecord, WorkerStatus, RolloutResult


@ray.remote
class Reconciler:
    def __init__(
        self,
        lease_seconds: float = 10.0,
        heartbeat_timeout_seconds: float = 5.0,
        max_retries: int = 2,
        trainer: Optional[Any] = None,
        max_policy_lag: int = 1,
    ) -> None:
        self.lease_seconds = lease_seconds
        self.heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self.max_retries = max_retries
        self.trainer = trainer
        self.max_policy_lag = max_policy_lag
        self.jobs: dict[str, JobRecord] = {}
        self.workers: dict[str, WorkerRecord] = {}
        self._next_job_index = 0

    def save_checkpoint(self, path: str) -> None:
        data = {
            "config": {
                "lease_seconds": self.lease_seconds,
                "heartbeat_timeout_seconds": self.heartbeat_timeout_seconds,
                "max_retries": self.max_retries,
                "max_policy_lag": self.max_policy_lag,
            },
            "_next_job_index": self._next_job_index,
            "jobs": {
                job_id: {
                    "job_id": job.job_id,
                    "status": job.status.value,
                    "policy_version": job.policy_version,
                    "assigned_worker": job.assigned_worker,
                    "attempt": job.attempt,
                    "lease_expiry": job.lease_expiry,
                    "num_retries": job.num_retries,
                    "result": job.result,
                }
                for job_id, job in self.jobs.items()
            },
            "workers": {
                worker_id: {
                    "worker_id": worker.worker_id,
                    "status": worker.status if isinstance(worker.status, str) else str(worker.status),
                    "last_heartbeat": worker.last_heartbeat,
                }
                for worker_id, worker in self.workers.items()
            },
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)

    def load_checkpoint(self, path: str) -> None:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        cfg = data.get("config", {})
        self.lease_seconds = float(cfg.get("lease_seconds", self.lease_seconds))
        self.heartbeat_timeout_seconds = float(
            cfg.get("heartbeat_timeout_seconds", self.heartbeat_timeout_seconds)
        )
        self.max_retries = int(cfg.get("max_retries", self.max_retries))
        self.max_policy_lag = int(cfg.get("max_policy_lag", self.max_policy_lag))

        self._next_job_index = int(data.get("_next_job_index", 0))

        self.jobs = {}
        for job_id, jd in (data.get("jobs", {}) or {}).items():
            status = JobStatus(jd["status"])
            self.jobs[job_id] = JobRecord(
                job_id=jd["job_id"],
                status=status,
                policy_version=int(jd["policy_version"]),
                assigned_worker=jd.get("assigned_worker"),
                attempt=int(jd.get("attempt", 0)),
                lease_expiry=jd.get("lease_expiry"),
                num_retries=int(jd.get("num_retries", 0)),
                result=jd.get("result"),
            )

        self.workers = {}
        for worker_id, wd in (data.get("workers", {}) or {}).items():
            self.workers[worker_id] = WorkerRecord(
                worker_id=wd["worker_id"],
                status=wd.get("status", WorkerStatus.ALIVE.value),
                last_heartbeat=float(wd.get("last_heartbeat", time.time())),
            )

        # Normalize restart state: don't resume LEASED jobs.
        for job in self.jobs.values():
            if job.status == JobStatus.LEASED:
                job.status = JobStatus.RETRY_PENDING if job.attempt > 0 else JobStatus.PENDING
            job.assigned_worker = None
            job.lease_expiry = None

    def seed_jobs(self, num_jobs: int, policy_version: Optional[int] = None) -> None:
        if policy_version is None:
            if self.trainer is not None:
                policy_version = ray.get(self.trainer.get_policy_version.remote())
            else:
                policy_version = 0

        for _ in range(num_jobs):
            job_id = f"job-{self._next_job_index}"
            self._next_job_index += 1
            self.jobs[job_id] = JobRecord(
                job_id=job_id, status=JobStatus.PENDING, policy_version=policy_version
            )

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

    def _trainer_policy_version(self) -> Optional[int]:
        if self.trainer is None:
            return None
        return ray.get(self.trainer.get_policy_version.remote())

    def _trainer_on_accepted_rollout(self, result: RolloutResult) -> None:
        # Side effect only: job is already COMPLETED. Fast path — no ray.get.
        if self.trainer is None:
            return
        self.trainer.submit_rollout.remote(
            {
                "job_id": result.job_id,
                "worker_id": result.worker_id,
                "attempt": result.attempt,
                "policy_version": result.policy_version,
                "payload": result.payload,
            }
        )

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

        current = self._trainer_policy_version()
        if current is not None:
            lag = current - job.policy_version
            if lag > self.max_policy_lag:
                print(
                    f"[reconciler] submit_result rejected: stale policy job_id={result.job_id} "
                    f"job_policy={job.policy_version} trainer_policy={current} lag={lag}"
                )
                job.status = JobStatus.STALE_POLICY
                job.lease_expiry = None
                job.assigned_worker = None
                return False

        job.status = JobStatus.COMPLETED
        job.result = result.payload
        print(
            f"[reconciler] job completed job_id={result.job_id} "
            f"worker_id={result.worker_id} attempt={result.attempt}"
        )
        self._trainer_on_accepted_rollout(result)
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

            current = self._trainer_policy_version()
            if current is not None:
                lag = current - job.policy_version
                if lag > self.max_policy_lag:
                    job.status = JobStatus.STALE_POLICY
                    job.lease_expiry = None
                    job.assigned_worker = None
                    print(
                        f"[reconciler] lease expired -> stale_policy job_id={job.job_id} "
                        f"job_policy={job.policy_version} trainer_policy={current} lag={lag}"
                    )
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
        terminal = {
            JobStatus.COMPLETED,
            JobStatus.FAILED_PERMANENT,
            JobStatus.STALE_POLICY,
        }
        return all(job.status in terminal for job in self.jobs.values())
