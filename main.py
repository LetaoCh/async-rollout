import time
import ray

from reconciler import Reconciler
from trainer import Trainer
from worker import RolloutWorker


def main() -> None:
    ray.init()

    trainer = Trainer.remote(train_batch_size=4)
    reconciler = Reconciler.remote(trainer, num_jobs=12)

    workers = [
        RolloutWorker.remote(f"worker-{i}", reconciler)
        for i in range(3)
    ]

    worker_refs = [w.run.remote() for w in workers]

    while True:
        stats = ray.get(reconciler.get_stats.remote())
        trainer_stats = ray.get(trainer.get_stats.remote())

        print("\n=== SYSTEM STATS ===")
        print(stats)
        print(trainer_stats)

        if stats["all_done"]:
            break

        time.sleep(1.0)

    ray.get(worker_refs)

    print("\nFinal reconciler stats:")
    print(ray.get(reconciler.get_stats.remote()))
    print("\nFinal trainer stats:")
    print(ray.get(trainer.get_stats.remote()))

    ray.shutdown()


if __name__ == "__main__":
    main()