"""PlatEMO-HPC Worker: lease one Master batch and run it in MATLAB."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import platform
import signal
import shutil
import asyncio.subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException
import uvicorn
import httpx


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_name(value: str, label: str) -> str:
    if not value or value != Path(value).name or any(c in value for c in "\\/\"';&|`$"):
        raise ValueError(f"invalid {label}")
    return value


class WorkerState:
    def __init__(self, config: dict[str, Any], config_path: Path) -> None:
        self.config = config
        self.config_path = config_path
        self.root = Path(config["platemo_root"]).expanduser().resolve()
        if not self.root.is_dir():
            raise RuntimeError(f"PlatEMO root does not exist: {self.root}")
        raw_data = Path(config.get("data_dir", "Data"))
        self.data = (config_path.parent / raw_data if not raw_data.is_absolute() else raw_data).resolve()
        self.queue = self.data / "queue"
        self.running = self.data / "running"
        self.work = self.data / "work"
        for directory in (self.queue, self.running, self.work):
            directory.mkdir(parents=True, exist_ok=True)
        self.worker_id = config.get("worker_id") or str(uuid.uuid4())
        self.node_token = str(config.get("node_token", ""))
        self.lock = asyncio.Lock()
        # One Worker owns one batch MATLAB process. Parallelism is inside the
        # MATLAB cluster profile, not multiple competing MATLAB processes.
        self.slots = asyncio.Semaphore(1)
        self.processes: dict[str, Any] = {}
        self.tasks: dict[str, asyncio.Task[Any]] = {}
        self.cancelled: set[str] = set()
        self.attempts: dict[str, dict[str, str]] = {}

    def save_config(self) -> None:
        self.config["worker_id"] = self.worker_id
        self.config["node_token"] = self.node_token
        self.config_path.write_text(json.dumps(self.config, indent=2), encoding="utf-8")

    def queued(self) -> list[Path]:
        return sorted(self.queue.glob("*.json"))

    def capabilities(self) -> dict[str, Any]:
        matlab = self.config.get("matlab_version", "unknown")
        return {
            "worker_id": self.worker_id,
            "name": self.config.get("worker_name", platform.node()),
            "version": self.config.get("worker_version", "1"),
            "cpu_logical": os.cpu_count() or 1,
            "disk_free_bytes": shutil.disk_usage(self.data).free,
            "gpu": self.config.get("gpu", []),
            "matlab_version": matlab,
            "parallel_computing_toolbox": bool(self.config.get("parallel_computing_toolbox", False)),
            "batch_mode": True,
            "platemo_root": str(self.root),
            "platemo_commit": self.config.get("platemo_commit", ""),
        }

    async def enqueue(self, payload: dict[str, Any]) -> dict[str, Any]:
        task_id = str(payload.get("id") or payload.get("task_id") or "")
        if not task_id:
            raise HTTPException(400, "Task id is required")
        try:
            seeds = payload.get("seeds")
            if not isinstance(seeds, list) or not seeds or any(not isinstance(seed, int) for seed in seeds):
                raise HTTPException(422, "A batch task requires integer seeds")
            if len(set(seeds)) != len(seeds):
                raise HTTPException(422, "seeds must be unique")
            if not isinstance(payload.get("max_fe"), int) or payload["max_fe"] < 1:
                raise HTTPException(422, "max_fe must be a positive integer")
            payload.setdefault("cluster_profile", "local")
            task_id = str(uuid.UUID(task_id))
        except ValueError as exc:
            raise HTTPException(400, "Task id must be a UUID") from exc
        target = self.queue / f"{task_id}.json"
        if target.exists() or (self.running / target.name).exists():
            raise HTTPException(409, "Task already exists")
        payload = dict(payload)
        payload.update({"id": task_id, "accepted_at": now(), "state": "queued"})
        try:
            for field in ("algorithm", "problem"):
                value = payload.get(field)
                name = value.get("name", "") if isinstance(value, dict) else value
                if not name:
                    raise HTTPException(422, f"{field} is required")
                _safe_name(str(name), field)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        async with self.lock:
            target.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        if self.config.get("auto_run"):
            self.start_task(task_id)
        return {"status": "queued", "task_id": task_id}

    def start_task(self, task_id: str) -> None:
        if task_id not in self.tasks:
            self.tasks[task_id] = asyncio.create_task(self.run_task(task_id))

    async def run_task(self, task_id: str) -> None:
        async with self.slots:
            await self._run_task(task_id)

    async def _run_task(self, task_id: str) -> None:
        source = self.queue / f"{task_id}.json"
        running = self.running / source.name
        payload: dict[str, Any] = {}
        attempt = ""
        lease_token = ""
        work_dir: Path | None = None
        try:
            async with self.lock:
                if not source.exists():
                    return
                source.replace(running)
            payload = json.loads(running.read_text(encoding="utf-8"))
            attempt = str(payload.get("attempt_id") or uuid.uuid4())
            lease_token = str(payload.get("lease_token", ""))
            self.attempts[task_id] = {"attempt_id": attempt, "lease_token": lease_token}
            work_dir = self.work / task_id / str(payload.get("attempt_no", 1))
            work_dir.mkdir(parents=True, exist_ok=True)
            task_json = work_dir / "task.json"
            task_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            queued_runs = [
                {"seed": seed, "state": "queued", "fe": 0, "total_fe": payload["max_fe"],
                 "elapsed_seconds": 0, "error": ""}
                for seed in payload["seeds"]
            ]
            (work_dir / "progress.json").write_text(json.dumps({
                "phase": "starting", "completed_runs": 0, "failed_runs": 0,
                "running_runs": 0, "total_runs": len(queued_runs), "runs": queued_runs,
                "pool": {"active": False, "type": "", "workers": 0,
                         "cluster_profile": payload["cluster_profile"]}, "timestamp": now(),
            }), encoding="utf-8")
            worker_dir = Path(__file__).resolve().parent
            expression = (
                f"addpath('{_matlab_quote(worker_dir)}');"
                f"run_task('{_matlab_quote(task_json)}','{_matlab_quote(work_dir)}')"
            )
            executable = str(self.config.get("matlab_exe", "matlab"))
            process = await asyncio.create_subprocess_exec(
                executable, "-batch", expression, cwd=str(self.root),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            )
            self.processes[task_id] = process
            progress_task = asyncio.create_task(self.report_progress(task_id, attempt, lease_token, work_dir))
            log_path = work_dir / "matlab.log"
            timeout = payload.get("timeout_seconds")
            try:
                output, _ = await asyncio.wait_for(process.communicate(), timeout=float(timeout)) if timeout else await process.communicate()
                code = process.returncode
            except asyncio.TimeoutError:
                process.terminate()
                output, _ = await asyncio.wait_for(process.communicate(), timeout=15)
                code = -signal.SIGTERM
            log_path.write_bytes(output or b"")
            progress_task.cancel()
            try:
                await progress_task
            except asyncio.CancelledError:
                pass
            result = work_dir / "result.mat"
            state = "completed" if code == 0 and result.is_file() and result.stat().st_size > 0 else "failed"
            if task_id in self.cancelled or payload.get("cancel_requested"):
                state = "cancelled"
            summary = {"task_id": task_id, "attempt_id": attempt, "state": state, "exit_code": code, "result": str(result) if result.is_file() else None, "sha256": _sha256(result) if result.is_file() else None, "finished_at": now()}
            progress_path = work_dir / "progress.json"
            if progress_path.is_file():
                try:
                    final_progress = json.loads(progress_path.read_text(encoding="utf-8"))
                    for key in ("runs", "completed_runs", "failed_runs", "running_runs", "total_runs", "pool"):
                        if key in final_progress:
                            summary[key] = final_progress[key]
                except (OSError, json.JSONDecodeError):
                    pass
            (work_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
            if result.is_file():
                await self.upload_artifact(task_id, attempt, lease_token, result)
            await self.complete_master(task_id, attempt, lease_token, summary)
            running.unlink(missing_ok=True)
        except Exception as exc:
            error_dir = work_dir or (self.work / task_id)
            error_dir.mkdir(parents=True, exist_ok=True)
            failure = {"task_id": task_id, "attempt_id": attempt, "state": "failed", "error": str(exc), "finished_at": now()}
            (error_dir / "worker-error.json").write_text(json.dumps(failure, indent=2), encoding="utf-8")
            if attempt and lease_token:
                await self.complete_master(task_id, attempt, lease_token, failure)
            running.unlink(missing_ok=True)
        finally:
            self.processes.pop(task_id, None)
            self.tasks.pop(task_id, None)
            self.attempts.pop(task_id, None)
            self.cancelled.discard(task_id)

    async def report_progress(self, task_id: str, attempt_id: str, lease_token: str, work_dir: Path) -> None:
        master_url = str(self.config.get("master_url", "")).rstrip("/")
        if not master_url or not self.node_token:
            return
        path = work_dir / "progress.json"
        while True:
            try:
                await asyncio.sleep(2)
                if not path.is_file():
                    continue
                progress = json.loads(path.read_text(encoding="utf-8"))
                progress["lease_token"] = lease_token
                async with httpx.AsyncClient(timeout=10) as client:
                    response = await client.post(f"{master_url}/api/v1/tasks/{task_id}/attempts/{attempt_id}/progress", json=progress, headers={"Authorization": f"Bearer {self.node_token}"})
                    response.raise_for_status()
            except asyncio.CancelledError:
                return
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 410:
                    return
            except (OSError, json.JSONDecodeError, httpx.HTTPError):
                continue

    async def upload_artifact(self, task_id: str, attempt_id: str, lease_token: str, result: Path) -> None:
        master_url = str(self.config.get("master_url", "")).rstrip("/")
        if not master_url or not self.node_token:
            return
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                with result.open("rb") as handle:
                    response = await client.post(
                        f"{master_url}/api/v1/artifacts/{uuid.uuid4()}",
                        data={"task_id": task_id, "attempt_id": attempt_id, "lease_token": lease_token, "kind": "result"},
                        files={"artifact": ("result.mat", handle, "application/octet-stream")},
                        headers={"Authorization": f"Bearer {self.node_token}"},
                    )
                    response.raise_for_status()
        except httpx.HTTPError as exc:
            (result.parent / "upload-error.txt").write_text(str(exc), encoding="utf-8")

    async def complete_master(self, task_id: str, attempt_id: str, lease_token: str, summary: dict[str, Any]) -> None:
        master_url = str(self.config.get("master_url", "")).rstrip("/")
        if not master_url or not self.node_token:
            return
        payload = dict(summary)
        payload["lease_token"] = lease_token
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(f"{master_url}/api/v1/tasks/{task_id}/attempts/{attempt_id}/complete", json=payload, headers={"Authorization": f"Bearer {self.node_token}"})
                response.raise_for_status()
        except httpx.HTTPError as exc:
            (self.work / task_id / "complete-upload-error.txt").write_text(str(exc), encoding="utf-8")

    async def register_with_master(self) -> None:
        master_url = str(self.config.get("master_url", "")).rstrip("/")
        join_token = str(self.config.get("master_join_token", ""))
        if self.node_token or not master_url or not join_token or "MASTER_ZEROTIER_IP" in master_url:
            return
        payload = {"worker_id": self.worker_id, "name": self.config.get("worker_name", platform.node()), "url": self.config.get("worker_url", ""), "capabilities": self.capabilities()}
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(f"{master_url}/api/v1/workers/register", json=payload, headers={"Authorization": f"Bearer {join_token}"})
            response.raise_for_status()
            self.node_token = response.json()["node_token"]
            self.save_config()

    async def control_loop(self) -> None:
        while True:
            try:
                await self.register_with_master()
                if self.node_token:
                    master_url = str(self.config.get("master_url", "")).rstrip("/")
                    headers = {"Authorization": f"Bearer {self.node_token}"}
                    running = [{"task_id": task_id, **value} for task_id, value in self.attempts.items()]
                    async with httpx.AsyncClient(timeout=10) as client:
                        heartbeat = await client.post(f"{master_url}/api/v1/workers/{self.worker_id}/heartbeat", json={"free_slots": 0 if self.processes else 1, "running": running, "capabilities": self.capabilities()}, headers=headers)
                        heartbeat.raise_for_status()
                        for task_id in heartbeat.json().get("cancel_task_ids", []):
                            await self.cancel(str(task_id))
                        if self.config.get("auto_run") and not self.processes:
                            response = await client.post(f"{master_url}/api/v1/workers/{self.worker_id}/lease", json={"free_slots": 1}, headers=headers)
                            response.raise_for_status()
                            lease = response.json()
                            if lease.get("task"):
                                task = dict(lease["task"])
                                task.update({"attempt_id": lease["attempt_id"], "lease_token": lease["lease_token"], "attempt_no": lease["attempt_no"]})
                                await self.enqueue(task)
                                self.start_task(task["id"])
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 401:
                    self.node_token = ""
                    self.save_config()
            except (httpx.HTTPError, OSError, HTTPException):
                pass
            await asyncio.sleep(10)

    async def cancel(self, task_id: str) -> bool:
        process = self.processes.get(task_id)
        if process and process.returncode is None:
            self.cancelled.add(task_id)
            if sys.platform == "win32":
                # MATLAB spawns helper processes; terminate() alone often leaves
                # its console process blocked and prevents the worker slot closing.
                killer = await asyncio.create_subprocess_exec(
                    "taskkill", "/PID", str(process.pid), "/T", "/F",
                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
                )
                await killer.wait()
            else:
                process.terminate()
            return True
        path = self.queue / f"{task_id}.json"
        if path.exists():
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["state"] = "cancelled"
            path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            path.unlink()
            return True
        return False


def _matlab_quote(path: Path) -> str:
    return str(path).replace("'", "''")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def create_app(config_path: Path) -> FastAPI:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    state = WorkerState(config, config_path)
    state.save_config()
    app = FastAPI(title="PlatEMO HPC Worker")
    app.state.worker = state

    def verify(token: str | None, authorization: str | None = None) -> None:
        expected = str(config.get("node_token", ""))
        supplied = token
        if authorization and authorization.lower().startswith("bearer "):
            supplied = authorization[7:]
        if expected and supplied != expected:
            raise HTTPException(401, "Invalid worker token")

    @app.on_event("startup")
    async def resume_queued_work() -> None:
        # In-progress folders remain untouched after a restart: their PIDs cannot
        # be trusted without an OS-level process identity check.
        if config.get("auto_run"):
            for queued in state.queued():
                state.start_task(queued.stem)
        app.state.control_task = asyncio.create_task(state.control_loop())

    @app.on_event("shutdown")
    async def stop_control_loop() -> None:
        task = getattr(app.state, "control_task", None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    @app.post("/api/v1/health")
    async def health(payload: dict[str, Any], x_worker_token: str | None = Header(None), authorization: str | None = Header(None)) -> dict[str, Any]:
        verify(x_worker_token, authorization)
        return {"status": "ready", "worker_id": state.worker_id, "queued": len(state.queued()), "running": len(state.processes), "capabilities": state.capabilities(), "time": now()}

    return app


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=Path(__file__).with_name("config.json"))
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=6001)
    args = parser.parse_args()
    uvicorn.run(create_app(args.config.resolve()), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
