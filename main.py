from __future__ import annotations

import os
import time

import ray

from reconciler import Reconciler
from trainer import Trainer
from worker import RolloutWorker


TERMINAL_STATUSES = ("COMPLETED", "FAILED_PERMANENT", "STALE_POLICY")


def done_total(summary: dict) -> tuple[int, int]:
    counts = summary["job_counts"]
    done = sum(counts.get(k, 0) for k in TERMINAL_STATUSES)
    total = sum(counts.values()) or 1
    return done, total


def tick_once(reconciler) -> dict:
    ray.get(reconciler.tick.remote())
    return ray.get(reconciler.get_summary.remote())


def main() -> None:
    ray.init()

    try:
        from tqdm import tqdm  # type: ignore
    except Exception:
        tqdm = None

    # Config (tweak here)
    num_jobs = 36
    num_workers = 6
    tick_seconds = 0.35
    warmup_seconds = 4.0
    recovery_demo = True

    trainer = Trainer.remote(initial_policy_version=0, train_every=3)
    reconciler = Reconciler.remote(
        lease_seconds=12.0,
        heartbeat_timeout_seconds=3.0,
        max_retries=2,
        trainer=trainer,
    )
    ray.get(reconciler.seed_jobs.remote(num_jobs=num_jobs))

    run_refs = [RolloutWorker.remote(f"worker-{i}", reconciler, verbose=False).run.remote() for i in range(num_workers)]

    pbar = None
    if tqdm is not None:
        pbar = tqdm(total=num_jobs, dynamic_ncols=True, desc="jobs", leave=True)

    def update_bar(summary: dict) -> None:
        nonlocal pbar
        done, total = done_total(summary)
        if tqdm is None:
            counts = summary["job_counts"]
            width = 30
            filled = int(width * done / total)
            bar = "#" * filled + "-" * (width - filled)
            print("\r" + f"[{bar}] {done}/{total} {counts}" + " " * 20, end="", flush=True)
        else:
            if pbar is None:
                pbar = tqdm(total=total, dynamic_ncols=True, desc="jobs", leave=True)
            pbar.total = total
            pbar.n = done
            pbar.refresh()

    if recovery_demo:
        trainer_ckpt = os.path.join(os.getcwd(), ".trainer_checkpoint.json")
        reconciler_ckpt = os.path.join(os.getcwd(), ".reconciler_checkpoint.json")

        start = time.time()
        while time.time() - start < warmup_seconds:
            summary = tick_once(reconciler)
            update_bar(summary)
            time.sleep(tick_seconds)

        ray.get(trainer.save_checkpoint.remote(trainer_ckpt))
        ray.get(reconciler.save_checkpoint.remote(reconciler_ckpt))
        msg = f"Saved checkpoints: {trainer_ckpt} {reconciler_ckpt}"
        if pbar is not None:
            pbar.write(msg)
        else:
            print()
            print(msg)

        ray.kill(reconciler)
        ray.kill(trainer)

        trainer = Trainer.remote()
        reconciler = Reconciler.remote(trainer=trainer)
        ray.get(trainer.load_checkpoint.remote(trainer_ckpt))
        ray.get(reconciler.load_checkpoint.remote(reconciler_ckpt))

        run_refs = [
            RolloutWorker.remote(f"recovered-worker-{i}", reconciler, verbose=False).run.remote()
            for i in range(num_workers)
        ]
        if pbar is not None:
            pbar.write("Recovered from checkpoints; continuing.")
        else:
            print()
            print("Recovered from checkpoints; continuing.")

    while True:
        summary = tick_once(reconciler)
        update_bar(summary)

        if ray.get(reconciler.all_jobs_finished.remote()):
            break

        time.sleep(tick_seconds)

    if pbar is not None:
        pbar.close()
    else:
        print()

    policy_version = ray.get(trainer.get_policy_version.remote())
    print(f"All jobs finished. Trainer policy_version={policy_version}")
    ray.shutdown()


if __name__ == "__main__":
    main()
