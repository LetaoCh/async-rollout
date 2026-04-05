from __future__ import annotations

import random
import time

import ray

from models import RolloutResult


@ray.remote
class RolloutWorker:
    def __init__(self, worker_id: str, reconciler):
        self.worker_id = worker_id
        self.reconciler = reconciler
        self.running = True

    def run(self) -> None:
        ray.get(self.reconciler.register_worker.remote(self.worker_id))

        while self.running:
            ray.get(self.reconciler.heartbeat.remote(self.worker_id))

            job = ray.get(self.reconciler.lease_job.remote(self.worker_id))

            if job is None:
                time.sleep(0.5)
                continue

            print(
                f"[{self.worker_id}] leased job {job.job_id} "
                f"attempt={job.attempt} policy_version={job.policy_version}"
            )

            # fake rollout work
            time.sleep(random.uniform(0.2, 1.0))

            print(f"[{self.worker_id}] finished fake rollout for {job.job_id}")

            result = RolloutResult(
                job_id=job.job_id,
                worker_id=self.worker_id,
                attempt=job.attempt,
                policy_version=job.policy_version,
                payload={
                    "reward": random.random(),
                    "steps": random.randint(10, 100),
                },
            )

            accepted = ray.get(self.reconciler.submit_result.remote(result))

            if accepted:
                print(f"[{self.worker_id}] result accepted for {job.job_id}")
            else:
                print(f"[{self.worker_id}] result rejected for {job.job_id}")

    # def stop(self) -> None:
    #     self.running = False
