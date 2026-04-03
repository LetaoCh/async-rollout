import time
import ray


@ray.remote
class Trainer:
    def __init__(self, train_batch_size: int = 4) -> None:
        self.train_batch_size = train_batch_size
        self.pending_rollouts = []
        self.policy_version = 0
        self.total_rollouts = 0
        self.total_updates = 0

    def submit_rollout(self, result: dict) -> int:
        self.pending_rollouts.append(result)
        self.total_rollouts += 1

        if len(self.pending_rollouts) >= self.train_batch_size:
            time.sleep(0.2)  # fake training work
            self.policy_version += 1
            self.total_updates += 1
            self.pending_rollouts.clear()
            print(f"[Trainer] completed update -> policy_version={self.policy_version}")

        return self.policy_version

    def get_policy_version(self) -> int:
        return self.policy_version

    def get_stats(self) -> dict:
        return {
            "policy_version": self.policy_version,
            "total_rollouts": self.total_rollouts,
            "total_updates": self.total_updates,
            "pending_rollouts": len(self.pending_rollouts),
        }