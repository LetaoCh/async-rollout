import random
import time
import ray


@ray.remote
class RolloutWorker:
    def __init__(self, worker_id: str, reconciler) -> None:
        self.worker_id = worker_id
        self.reconciler = reconciler

    def run(self) -> None:
        ray.get(self.reconciler.register_worker.remote(self.worker_id))

        while True:
            job = ray.get(self.reconciler.lease_job.remote(self.worker_id))
            if job is None:
                print(f"[Worker {self.worker_id}] no more work, exiting")
                return

            print(
                f"[Worker {self.worker_id}] running {job['job_id']} "
                f"attempt={job['attempt']} policy_version={job['policy_version']}"
            )

            # fake rollout
            time.sleep(random.uniform(0.3, 1.0))
            result = {
                "job_id": job["job_id"],
                "worker_id": self.worker_id,
                "policy_version": job["policy_version"],
                "reward": round(random.uniform(0.0, 1.0), 3),
                "steps": random.randint(20, 100),
            }

            accepted = ray.get(
                self.reconciler.submit_result.remote(
                    self.worker_id,
                    job["job_id"],
                    job["attempt"],
                    result,
                )
            )

            print(f"[Worker {self.worker_id}] submit result for {job['job_id']} accepted={accepted}")