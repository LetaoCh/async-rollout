from __future__ import annotations

import random
import time

import ray

from models import RolloutResult


@ray.remote
class RolloutWorker:
    def __init__(self, worker_id: str, reconciler, verbose: bool = False):
        self.worker_id = worker_id
        self.reconciler = reconciler
        self.running = True
        self.verbose = verbose

    def run(self) -> None:
        ray.get(self.reconciler.register_worker.remote(self.worker_id))

        while self.running:
            ray.get(self.reconciler.heartbeat.remote(self.worker_id))

            job = ray.get(self.reconciler.lease_job.remote(self.worker_id))

            if job is None:
                time.sleep(0.5)
                continue

            if self.verbose:
                print(
                    f"[{self.worker_id}] leased job {job.job_id} "
                    f"attempt={job.attempt} policy_version={job.policy_version}"
                )

            # Mixed-run sim: mostly fast completions; some slow paths stress lease + policy lag.
            if random.random() < 0.70:
                delay = random.uniform(0.15, 0.85)
            else:
                delay = random.uniform(9.0, 22.0)
                if self.verbose:
                    print(f"[{self.worker_id}] long-hang rollout {delay:.1f}s for {job.job_id}")

            time.sleep(delay)

            if self.verbose:
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

            if self.verbose:
                if accepted:
                    print(f"[{self.worker_id}] result accepted for {job.job_id}")
                else:
                    print(f"[{self.worker_id}] result rejected for {job.job_id}")