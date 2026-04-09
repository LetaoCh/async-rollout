import json
import ray


@ray.remote
class Trainer:
    def __init__(
        self,
        initial_policy_version: int = 0,
        train_every: int = 4,
        buffer_max: int = 16,
    ):
        self.current_policy_version = initial_policy_version
        self.train_every = train_every
        self.num_rollouts_received = 0
        self.buffer_max = buffer_max
        self.recent_rollouts: list[dict] = []

    def save_checkpoint(self, path: str) -> None:
        data = {
            "current_policy_version": self.current_policy_version,
            "num_rollouts_received": self.num_rollouts_received,
            "buffer_max": self.buffer_max,
            "recent_rollouts": self.recent_rollouts,
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)

    def load_checkpoint(self, path: str) -> None:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.current_policy_version = int(data.get("current_policy_version", 0))
        self.num_rollouts_received = int(data.get("num_rollouts_received", 0))
        self.buffer_max = int(data.get("buffer_max", self.buffer_max))
        self.recent_rollouts = list(data.get("recent_rollouts", []))
        if len(self.recent_rollouts) > self.buffer_max:
            self.recent_rollouts = self.recent_rollouts[-self.buffer_max :]

    def submit_rollout(self, rollout: dict) -> int:
        self.num_rollouts_received += 1
        self.recent_rollouts.append(rollout)
        if len(self.recent_rollouts) > self.buffer_max:
            self.recent_rollouts.pop(0)

        if self.num_rollouts_received % self.train_every == 0:
            self.current_policy_version += 1
            print(
                f"[trainer] advanced policy_version={self.current_policy_version} "
                f"after num_rollouts_received={self.num_rollouts_received}"
            )

        return self.current_policy_version

    def get_policy_version(self) -> int:
        return self.current_policy_version