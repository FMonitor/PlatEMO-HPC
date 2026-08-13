"""PlatEMO-HPC Worker: execute Master-assigned Seed batches in MATLAB."""
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
        self.invalidated_batches: set[str] = set()
        self.batches: dict[str, dict[str, Any]] = {}
        self.configured_pool_workers = 0
        self.actual_pool_workers = 0

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
            "cluster_profile": self.config.get("cluster_profile", "local"),
            "configured_pool_workers": self.configured_pool_workers,
        }

    def max_seeds_per_batch(self) -> int:
        configured = int(self.config.get("max_seeds_per_batch", 0) or 0)
        pool_workers = self.configured_pool_workers
        if pool_workers < 1:
            return 0
        return max(1, min(configured, pool_workers)) if configured else pool_workers

    def can_accept_assignment(self) -> bool:
        return bool(self.config.get("auto_run")) and self.configured_pool_workers > 0 and not self.processes and not self.tasks

    async def probe_pool_capacity(self) -> None:
        """Read the configured MATLAB profile without creating a parpool."""
        profile = _safe_name(str(self.config.get("cluster_profile", "local")), "cluster profile")
        matlab = str(self.config.get("matlab_exe", "matlab"))
        marker = "PLATEMO_HPC_POOL_WORKERS="
        expression = f"p=parcluster('{_matlab_quote(profile)}');fprintf('{marker}%d\\n',p.NumWorkers);exit"
        try:
            process = await asyncio.create_subprocess_exec(
                matlab, "-batch", expression, cwd=str(self.root),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            )
            output, _ = await asyncio.wait_for(process.communicate(), timeout=45)
            for line in (output or b"").decode(errors="replace").splitlines():
                if line.startswith(marker):
                    self.configured_pool_workers = max(0, int(line.removeprefix(marker).strip()))
                    return
        except (OSError, ValueError, asyncio.TimeoutError):
            pass

    async def enqueue(self, payload: dict[str, Any]) -> dict[str, Any]:
        batch_id = str(payload.get("batch_attempt_id") or "")
        point_id = str(payload.get("experiment_point_id") or "")
        if not batch_id or not point_id:
            raise HTTPException(400, "batch_attempt_id and experiment_point_id are required")
        try:
            local_profile = _safe_name(str(self.config.get("cluster_profile", "local")), "cluster profile")
            assigned_profile = _safe_name(str(payload.get("cluster_profile", local_profile)), "cluster profile")
        except ValueError as exc:
            raise HTTPException(422, "profile_unavailable") from exc
        if assigned_profile != local_profile:
            raise HTTPException(422, "profile_unavailable")
        try:
            seeds = payload.get("seeds")
            if not isinstance(seeds, list) or not seeds or any(not isinstance(seed, int) for seed in seeds):
                raise HTTPException(422, "A batch task requires integer seeds")
            if len(set(seeds)) != len(seeds):
                raise HTTPException(422, "seeds must be unique")
            if self.configured_pool_workers < 1:
                raise HTTPException(422, "profile_unavailable")
            if len(seeds) > self.max_seeds_per_batch():
                raise HTTPException(422, "assignment exceeds max_seeds_per_batch")
            if not isinstance(payload.get("max_fe"), int) or payload["max_fe"] < 1:
                raise HTTPException(422, "max_fe must be a positive integer")
            payload["cluster_profile"] = local_profile
            batch_id = str(uuid.UUID(batch_id))
            point_id = str(uuid.UUID(point_id))
            if not str(payload.get("lease_token", "")):
                raise HTTPException(422, "lease_token is required")
        except ValueError as exc:
            raise HTTPException(400, "batch_attempt_id and experiment_point_id must be UUIDs") from exc
        target = self.queue / f"{batch_id}.json"
        if target.exists() or (self.running / target.name).exists():
            return {"status": "accepted", "batch_attempt_id": batch_id}
        payload = dict(payload)
        payload.update({"batch_attempt_id": batch_id, "experiment_point_id": point_id,
                        "accepted_at": now(), "state": "queued"})
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
            self.start_task(batch_id)
        return {"status": "accepted", "batch_attempt_id": batch_id}

    def start_task(self, batch_id: str) -> None:
        if batch_id not in self.tasks:
            self.tasks[batch_id] = asyncio.create_task(self.run_task(batch_id))

    async def run_task(self, batch_id: str) -> None:
        async with self.slots:
            await self._run_task(batch_id)

    async def _run_task(self, batch_id: str) -> None:
        source = self.queue / f"{batch_id}.json"
        running = self.running / source.name
        payload: dict[str, Any] = {}
        point_id = ""
        lease_token = ""
        work_dir: Path | None = None
        try:
            async with self.lock:
                if not source.exists():
                    return
                source.replace(running)
            payload = json.loads(running.read_text(encoding="utf-8"))
            point_id = str(payload["experiment_point_id"])
            lease_token = str(payload.get("lease_token", ""))
            self.batches[batch_id] = {"experiment_point_id": point_id, "lease_token": lease_token,
                                      "seeds": payload["seeds"], "matlab_pid": 0}
            work_dir = self.work / point_id / batch_id
            work_dir.mkdir(parents=True, exist_ok=True)
            task_json = work_dir / "task.json"
            task_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            queued_runs = [
                {"seed": seed, "state": "queued", "fe": 0, "total_fe": payload["max_fe"],
                 "elapsed_seconds": 0, "error": ""}
                for seed in payload["seeds"]
            ]
            (work_dir / "progress.json").write_text(json.dumps({
                "phase": "accepted", "completed_runs": 0, "failed_runs": 0,
                "running_runs": 0, "total_runs": len(queued_runs), "runs": queued_runs,
                "pool": {"active": False, "type": "", "workers": 0,
                         "cluster_profile": payload["cluster_profile"]}, "timestamp": now(),
            }), encoding="utf-8")
            if await self.send_progress(batch_id, lease_token, work_dir) is not True:
                running.unlink(missing_ok=True)
                return
            if batch_id in self.invalidated_batches:
                running.unlink(missing_ok=True)
                return
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
            self.processes[batch_id] = process
            self.batches[batch_id]["matlab_pid"] = process.pid
            progress_task = asyncio.create_task(self.report_progress(batch_id, lease_token, work_dir))
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
            if batch_id in self.cancelled or payload.get("cancel_requested"):
                state = "cancelled"
            summary = {"experiment_point_id": point_id, "batch_attempt_id": batch_id, "state": state,
                       "exit_code": code, "result": str(result) if result.is_file() else None,
                       "sha256": _sha256(result) if result.is_file() else None, "finished_at": now()}
            progress_path = work_dir / "progress.json"
            final_progress: dict[str, Any] = {}
            if progress_path.is_file():
                try:
                    final_progress = json.loads(progress_path.read_text(encoding="utf-8"))
                    for key in ("runs", "completed_runs", "failed_runs", "running_runs", "total_runs", "pool"):
                        if key in final_progress:
                            summary[key] = final_progress[key]
                except (OSError, json.JSONDecodeError):
                    pass
            if state == "cancelled":
                self._mark_unfinished_cancelled(summary, payload["seeds"], payload["max_fe"])
                progress_path.write_text(json.dumps({
                    **final_progress,
                    "phase": "cancelled",
                    "runs": summary["runs"],
                    "completed_runs": summary["completed_runs"],
                    "failed_runs": summary["failed_runs"],
                    "running_runs": 0,
                    "total_runs": summary["total_runs"],
                    "timestamp": now(),
                }), encoding="utf-8")
            (work_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
            if batch_id in self.invalidated_batches:
                running.unlink(missing_ok=True)
                return
            if result.is_file():
                await self.upload_artifact(point_id, batch_id, lease_token, result, payload["seeds"])
            if batch_id not in self.invalidated_batches:
                await self.complete_master(batch_id, lease_token, summary)
            running.unlink(missing_ok=True)
        except Exception as exc:
            error_dir = work_dir or (self.work / batch_id)
            error_dir.mkdir(parents=True, exist_ok=True)
            failure = {"experiment_point_id": point_id, "batch_attempt_id": batch_id,
                       "state": "failed", "error": str(exc), "finished_at": now()}
            (error_dir / "worker-error.json").write_text(json.dumps(failure, indent=2), encoding="utf-8")
            if point_id and lease_token and batch_id not in self.invalidated_batches:
                await self.complete_master(batch_id, lease_token, failure)
            running.unlink(missing_ok=True)
        finally:
            self.processes.pop(batch_id, None)
            self.tasks.pop(batch_id, None)
            self.batches.pop(batch_id, None)
            self.cancelled.discard(batch_id)
            self.invalidated_batches.discard(batch_id)
            if not self.processes:
                self.actual_pool_workers = 0

    async def report_progress(self, batch_id: str, lease_token: str, work_dir: Path) -> None:
        while True:
            try:
                await asyncio.sleep(2)
                if await self.send_progress(batch_id, lease_token, work_dir) is False:
                    return
            except asyncio.CancelledError:
                return

    async def send_progress(self, batch_id: str, lease_token: str, work_dir: Path) -> bool | None:
        if batch_id in self.invalidated_batches:
            return False
        master_url = str(self.config.get("master_url", "")).rstrip("/")
        path = work_dir / "progress.json"
        if not master_url or not self.node_token or not path.is_file():
            return None
        try:
            progress = json.loads(path.read_text(encoding="utf-8"))
            progress["lease_token"] = lease_token
            pool = progress.get("pool", {})
            if isinstance(pool, dict):
                workers = pool.get("workers", 0)
                if isinstance(workers, int) and workers >= 0:
                    self.actual_pool_workers = workers
                    batch = self.batches.get(batch_id)
                    if batch is not None:
                        batch["actual_pool_workers"] = workers
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(f"{master_url}/api/v1/batch-attempts/{batch_id}/progress", json=progress, headers={"Authorization": f"Bearer {self.node_token}"})
                response.raise_for_status()
            return True
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 410:
                await self.invalidate_batch(batch_id)
                return False
            return None
        except (OSError, json.JSONDecodeError, httpx.HTTPError):
            return None

    async def upload_artifact(self, point_id: str, batch_id: str, lease_token: str, result: Path, seeds: list[int]) -> None:
        master_url = str(self.config.get("master_url", "")).rstrip("/")
        if batch_id in self.invalidated_batches or not master_url or not self.node_token:
            return
        try:
            fields = {
                "experiment_point_id": point_id,
                "batch_attempt_id": batch_id,
                "lease_token": lease_token,
                "kind": "batch_result",
            }
            if len(seeds) == 1:
                fields["seed"] = str(seeds[0])
            async with httpx.AsyncClient(timeout=120) as client:
                with result.open("rb") as handle:
                    response = await client.put(
                        f"{master_url}/api/v1/artifacts/{uuid.uuid4()}",
                        data=fields,
                        files={"artifact": ("result.mat", handle, "application/octet-stream")},
                        headers={"Authorization": f"Bearer {self.node_token}"},
                    )
                    response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 410:
                await self.invalidate_batch(batch_id)
                return
            (result.parent / "upload-error.txt").write_text(str(exc), encoding="utf-8")
        except httpx.HTTPError as exc:
            (result.parent / "upload-error.txt").write_text(str(exc), encoding="utf-8")

    async def complete_master(self, batch_id: str, lease_token: str, summary: dict[str, Any]) -> None:
        master_url = str(self.config.get("master_url", "")).rstrip("/")
        if batch_id in self.invalidated_batches or not master_url or not self.node_token:
            return
        payload = dict(summary)
        payload["lease_token"] = lease_token
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(f"{master_url}/api/v1/batch-attempts/{batch_id}/complete", json=payload, headers={"Authorization": f"Bearer {self.node_token}"})
                response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 410:
                await self.invalidate_batch(batch_id)
                return
            error_path = self.work / batch_id / "complete-upload-error.txt"
            error_path.parent.mkdir(parents=True, exist_ok=True)
            error_path.write_text(str(exc), encoding="utf-8")
        except httpx.HTTPError as exc:
            error_path = self.work / batch_id / "complete-upload-error.txt"
            error_path.parent.mkdir(parents=True, exist_ok=True)
            error_path.write_text(str(exc), encoding="utf-8")

    async def invalidate_batch(self, batch_id: str) -> None:
        """Stop an expired lease and leave its files intact for Master recovery."""
        if batch_id in self.invalidated_batches:
            return
        self.invalidated_batches.add(batch_id)
        process = self.processes.get(batch_id)
        if process and process.returncode is None:
            await self._terminate_process(process)

    @staticmethod
    def _mark_unfinished_cancelled(summary: dict[str, Any], seeds: list[int], max_fe: int) -> None:
        terminal = {"completed", "failed", "cancelled"}
        known = {item.get("seed"): dict(item) for item in summary.get("runs", [])
                 if isinstance(item, dict) and isinstance(item.get("seed"), int)}
        runs: list[dict[str, Any]] = []
        for seed in seeds:
            run = known.get(seed, {"seed": seed, "fe": 0, "total_fe": max_fe, "elapsed_seconds": 0, "error": ""})
            if str(run.get("state", "queued")) not in terminal:
                run["state"] = "cancelled"
                run["error"] = str(run.get("error") or "cancelled by Master")
            runs.append(run)
        summary["runs"] = runs
        summary["completed_runs"] = sum(run.get("state") == "completed" for run in runs)
        summary["failed_runs"] = sum(run.get("state") == "failed" for run in runs)
        summary["running_runs"] = 0
        summary["total_runs"] = len(runs)

    async def reject_assignment(self, assignment: dict[str, Any], code: str, message: str) -> None:
        master_url = str(self.config.get("master_url", "")).rstrip("/")
        batch_id = str(assignment.get("batch_attempt_id", ""))
        lease_token = str(assignment.get("lease_token", ""))
        if not master_url or not self.node_token or not batch_id or not lease_token:
            return
        payload = {"lease_token": lease_token, "phase": "rejected", "error_code": code,
                   "error": message, "runs": []}
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(f"{master_url}/api/v1/batch-attempts/{batch_id}/progress", json=payload, headers={"Authorization": f"Bearer {self.node_token}"})
                response.raise_for_status()
        except httpx.HTTPError:
            pass

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
                    running = [{"batch_attempt_id": batch_id, **value} for batch_id, value in self.batches.items()]
                    available = 1 if self.can_accept_assignment() else 0
                    heartbeat_payload = {
                        "available_batch_slots": available,
                        "max_concurrent_batches": 1,
                        "configured_pool_workers": self.configured_pool_workers,
                        "actual_pool_workers": self.actual_pool_workers,
                        "max_seeds_per_batch": self.max_seeds_per_batch(),
                        "running_batches": running,
                        "capabilities": self.capabilities(),
                    }
                    async with httpx.AsyncClient(timeout=10) as client:
                        heartbeat = await client.post(f"{master_url}/api/v1/workers/{self.worker_id}/heartbeat", json=heartbeat_payload, headers=headers)
                        heartbeat.raise_for_status()
                        response = heartbeat.json()
                        for batch_id in response.get("cancel_batch_attempt_ids", []):
                            await self.cancel(str(batch_id))
                        assignment = response.get("assignment")
                        if assignment and self.can_accept_assignment():
                            try:
                                await self.enqueue(dict(assignment))
                            except HTTPException as exc:
                                error_code = "profile_unavailable" if exc.detail == "profile_unavailable" else "input_incompatible"
                                await self.reject_assignment(dict(assignment), error_code, str(exc.detail))
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 401:
                    self.node_token = ""
                    self.save_config()
            except (httpx.HTTPError, OSError, HTTPException):
                pass
            await asyncio.sleep(10)

    async def cancel(self, batch_id: str) -> bool:
        process = self.processes.get(batch_id)
        if process and process.returncode is None:
            self.cancelled.add(batch_id)
            await self._terminate_process(process)
            return True
        path = self.queue / f"{batch_id}.json"
        if path.exists():
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["state"] = "cancelled"
            path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            path.unlink()
            await self.complete_master(batch_id, str(payload.get("lease_token", "")), {
                "experiment_point_id": str(payload.get("experiment_point_id", "")),
                "batch_attempt_id": batch_id,
                "state": "cancelled",
                "exit_code": None,
                "runs": [{"seed": seed, "state": "cancelled", "fe": 0,
                          "total_fe": payload.get("max_fe", 0), "elapsed_seconds": 0,
                          "error": "cancelled before MATLAB start"} for seed in payload.get("seeds", [])],
                "finished_at": now(),
            })
            return True
        return False

    async def _terminate_process(self, process: Any) -> None:
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
        # Never execute persisted assignments after a restart. Their lease may
        # have been reclaimed while this Worker was down; keep files for audit.
        await state.probe_pool_capacity()
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
        return {"status": "ready", "worker_id": state.worker_id, "queued": len(state.queued()),
                "running": len(state.processes),
                "available_batch_slots": 1 if state.can_accept_assignment() else 0,
                "configured_pool_workers": state.configured_pool_workers,
                "max_seeds_per_batch": state.max_seeds_per_batch(),
                "capabilities": state.capabilities(), "time": now()}

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
