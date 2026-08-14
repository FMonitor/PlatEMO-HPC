"""CLI compatibility entrypoint for the modular PlatEMO-HPC Worker."""
from __future__ import annotations

import argparse
from pathlib import Path

import uvicorn

from platemo_worker.api import create_app
from platemo_worker.runtime import WorkerState
from platemo_worker.version import WORKER_VERSION

__all__ = ["WorkerState", "create_app"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=Path(__file__).with_name("config.json"))
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=6001)
    args = parser.parse_args()
    print(
        f"[PlatEMO-HPC Worker] version={WORKER_VERSION} "
        f"config={args.config.resolve()} mode=loading",
        flush=True,
    )
    uvicorn.run(create_app(args.config.resolve()), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
