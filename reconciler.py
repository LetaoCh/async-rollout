from __future__ import annotations

from typing import Optional
import ray

from models import Job, JobStatus


@ray.remote
class Reconciler:
    def __init__(self, trainer, num_jobs: int = 20) -> None:
        self.trainer = trainer
        self.jobs: dict[str, Job] = {}
        self.workers: set[str] = set()

        initial_policy_version = ray.get(self.trainer.get_policy_version.remote())
        for i in range(num_jobs):
            job_id = f"job-{i}"
            self.jobs[job_id] = Job(
                job_id=job_id,
                status=JobStatus.PENDING,
                policy_version=initial_policy_version,
            )

    def register_worker(self, worker_id: str) -> None:
        self.workers.add(worker_id)
        print(f"[Reconciler] registered worker={worker_id}")

    def lease_job(self, worker_id: str) -> Optional[dict]:
        for job in self.jobs.values():
            if job.status == JobStatus.PENDING:
                job.status = JobStatus.LEASED
                job.assigned_worker = worker_id
                job.attempt += 1

                print(
                    f"[Reconciler] leased {job.job_id} "
                    f"to worker={worker_id} attempt={job.attempt} "
                    f"policy_version={job.policy_version}"
                )

                return {
                    "job_id": job.job_id,
                    "policy_version": job.policy_version,
                    "attempt": job.attempt,
                }
        return None

    def submit_result(self, worker_id: str, job_id: str, attempt: int, result: dict) -> bool:
        job = self.jobs[job_id]

        # V1 stale-result protection
        if job.status != JobStatus.LEASED:
            print(f"[Reconciler] reject result for {job_id}: status={job.status}")
            return False

        if job.assigned_worker != worker_id:
            print(
                f"[Reconciler] reject result for {job_id}: "
                f"worker mismatch current={job.assigned_worker} got={worker_id}"
            )
            return False

        if job.attempt != attempt:
            print(
                f"[Reconciler] reject result for {job_id}: "
                f"attempt mismatch current={job.attempt} got={attempt}"
            )
            return False

        job.status = JobStatus.COMPLETED
        job.result = result

        new_policy_version = ray.get(self.trainer.submit_rollout.remote(result))

        print(
            f"[Reconciler] accepted result for {job_id} from worker={worker_id}. "
            f"trainer_policy_version={new_policy_version}"
        )
        return True

    def get_stats(self) -> dict:
        counts = {
            "PENDING": 0,
            "LEASED": 0,
            "COMPLETED": 0,
        }
        for job in self.jobs.values():
            counts[job.status.value] += 1

        return {
            "workers": sorted(self.workers),
            "job_counts": counts,
            "all_done": counts["COMPLETED"] == len(self.jobs),
            "num_jobs": len(self.jobs),
        }