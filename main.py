from __future__ import annotations

import time

import ray

from reconciler import Reconciler
from worker import RolloutWorker


def main() -> None:
    ray.init()

    reconciler = Reconciler.remote(lease_seconds=10.0)
    ray.get(reconciler.seed_jobs.remote(num_jobs=10, policy_version=0))

    workers = [RolloutWorker.remote(f"worker-{i}", reconciler) for i in range(3)]

    # start workers
    run_refs = [worker.run.remote() for worker in workers]

    while True:
        summary = ray.get(reconciler.get_summary.remote())
        print(summary)

        done = ray.get(reconciler.all_jobs_completed.remote())
        if done:
            break

        time.sleep(1.0)

    # for worker in workers:
    #     ray.get(worker.stop.remote())

    print("All jobs completed.")

    ray.shutdown()


if __name__ == "__main__":
    main()
