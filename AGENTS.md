# AGENTS.md

## Cursor Cloud specific instructions

### Overview

This is a small Python learning project simulating asynchronous RL rollouts using Ray actors. There is a single entry point (`python3 main.py`) and no external services (databases, Docker, etc.) are required. Ray starts a local cluster in-process via `ray.init()`.

### Running

```bash
python3 main.py
```

The simulation runs 36 jobs across 6 workers with a checkpoint/recovery demo and completes in ~60-90 seconds. Expected output includes `ray.kill` errors during the recovery demo — these are intentional. The run finishes with `All jobs finished. Trainer policy_version=<N>`.

### Lint / Test

No automated test suite or lint configuration exists in the repo. Use `python3 -m py_compile <file>.py` for syntax checks. The project has no `pyproject.toml`, `setup.cfg`, or linter configs.

### Gotchas

- Use `python3` not `python` (the VM may not have `python` on PATH).
- `pip install` uses `--user` by default in this environment; the installed packages go to `~/.local/lib/python3.12/`.
- The simulation writes ephemeral checkpoint files (`.trainer_checkpoint.json`, `.reconciler_checkpoint.json`) to CWD during the recovery demo — these are gitignored.
