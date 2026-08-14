"""PlatEMO-HPC Worker: execute Master-assigned Seed batches in MATLAB."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import platform
import signal
import shutil
import asyncio.subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException
import uvicorn
import httpx

from platemo_worker.settings import resolve_settings
from platemo_worker.summary import build_summary, mark_unfinished_cancelled, read_progress
from platemo_worker.logging_utils import create_worker_logger
from platemo_worker.version import WORKER_VERSION


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
        self.log: logging.Logger = create_worker_logger(self.data, self.worker_id)
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
        self.orphaned_pids: dict[str, int] = {}
        self.configured_pool_workers = 0
        self.actual_pool_workers = 0
        self.pool_probe_task: asyncio.Task[bool] | None = None
        self.pool_probe_process: asyncio.subprocess.Process | None = None
        self.next_pool_probe_at = 0.0
        self.pool_probe_error = ""
        self.master_connected: bool | None = None
        self.log.info("worker initialized id=%s profile=%s data=%s", self.worker_id, config.get("cluster_profile", "local"), self.data)

    def set_master_connection(self, connected: bool, detail: str = "") -> None:
        """Log connection transitions without emitting one line per heartbeat."""
        if self.master_connected is connected:
            return
        self.master_connected = connected
        if connected:
            self.log.info("Master connection restored")
        else:
            self.log.warning("Master connection unavailable%s", f": {detail}" if detail else "")

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
            "version": WORKER_VERSION,
            "cpu_logical": os.cpu_count() or 1,
            "disk_free_bytes": shutil.disk_usage(self.data).free,
            "gpu": self.config.get("gpu", []),
            "matlab_version": matlab,
            "parallel_computing_toolbox": bool(self.config.get("parallel_computing_toolbox", False)),
            "execution_mode": "dynamic_seed_session",
            "batch_mode": False,
            "platemo_root": str(self.root),
            "platemo_commit": self.config.get("platemo_commit", ""),
            "cluster_profile": self.config.get("cluster_profile", "local"),
            "configured_pool_workers": self.configured_pool_workers,
            "profile_ready": self.configured_pool_workers > 0,
        }

    def max_seeds_per_batch(self) -> int:
        configured = int(self.config.get("max_seeds_per_batch", 0) or 0)
        pool_workers = self.configured_pool_workers
        if pool_workers < 1:
            return 0
        return max(1, min(configured, pool_workers)) if configured else pool_workers

    def can_accept_assignment(self) -> bool:
        return bool(self.config.get("auto_run")) and self.configured_pool_workers > 0 and not self.processes and not self.tasks and not self.batches

    def validate_environment(self, payload: dict[str, Any]) -> None:
        try:
            payload.update(resolve_settings(self.root, payload))
        except (OSError, ValueError):
            raise HTTPException(422, "input_incompatible")
        required_commit = str(payload.get("required_platemo_commit", "") or "")
        actual_commit = str(self.config.get("platemo_commit", "") or "")
        if required_commit and required_commit != actual_commit:
            raise HTTPException(422, "version_mismatch")
        try:
            minimum_disk = int(payload.get("minimum_disk_free_bytes", 0) or 0)
        except (TypeError, ValueError):
            raise HTTPException(422, "input_incompatible")
        if minimum_disk < 0 or shutil.disk_usage(self.data).free < minimum_disk:
            raise HTTPException(422, "insufficient_disk")

    async def prepare_assignment_settings(self, payload: dict[str, Any]) -> None:
        """Fetch a Master-uploaded MAT into local PlatEMO/Data and verify its digest."""
        download_url = str(payload.get("settings_download_url", "") or "")
        expected = str(payload.get("settings_sha256", "") or "")
        filename = str(payload.get("settings_file", "") or "")
        if not download_url:
            return
        try:
            filename = _safe_name(filename, "settings_file")
        except ValueError as exc:
            raise HTTPException(422, "input_incompatible") from exc
        if not expected:
            raise HTTPException(422, "input_incompatible")
        target = self.root / "Data" / filename
        if target.is_file() and _sha256(target) == expected:
            return
        master_url = str(self.config.get("master_url", "")).rstrip("/")
        if not master_url or not self.node_token:
            raise HTTPException(422, "input_incompatible")
        url = download_url if download_url.startswith("http://") or download_url.startswith("https://") else f"{master_url}{download_url}"
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                response = await client.get(url, headers={"Authorization": f"Bearer {self.node_token}"})
                response.raise_for_status()
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_suffix(target.suffix + ".download")
            temporary.write_bytes(response.content)
            if _sha256(temporary) != expected:
                temporary.unlink(missing_ok=True)
                raise HTTPException(422, "input_incompatible")
            temporary.replace(target)
        except HTTPException:
            raise
        except (OSError, httpx.HTTPError) as exc:
            raise HTTPException(422, "input_incompatible") from exc

    @staticmethod
    def _process_record_path(work_dir: Path) -> Path:
        return work_dir / "process.json"

    @staticmethod
    def _launch_expression(task_json: Path, work_dir: Path) -> str:
        # MATLAB wrappers live beside the Python package, not inside it.
        worker_dir = Path(__file__).resolve().parent.parent
        return (
            f"addpath('{_matlab_quote(worker_dir)}');"
            f"run_task('{_matlab_quote(task_json)}','{_matlab_quote(work_dir)}')"
        )

    @staticmethod
    def _command_fingerprint(expression: str) -> str:
        return hashlib.sha256(expression.encode("utf-8")).hexdigest()

    def _write_process_record(self, work_dir: Path, batch_id: str, lease_token: str, process: Any, command: str) -> None:
        record = {
            "batch_attempt_id": batch_id,
            "lease_token": lease_token,
            "pid": process.pid,
            "command_sha256": self._command_fingerprint(command),
            "task_json": str(work_dir / "task.json"),
            "created_at": now(),
        }
        self._process_record_path(work_dir).write_text(json.dumps(record, indent=2), encoding="utf-8")

    async def _pid_command_line(self, pid: int) -> str | None:
        if pid < 1:
            return None
        if sys.platform != "win32":
            return None
        command = f"$p=Get-CimInstance Win32_Process -Filter 'ProcessId = {pid}'; if ($null -ne $p) {{ $p.CommandLine }}"
        try:
            process = await asyncio.create_subprocess_exec(
                "powershell", "-NoProfile", "-NonInteractive", "-Command", command,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            )
            output, _ = await asyncio.wait_for(process.communicate(), timeout=10)
            value = (output or b"").decode(errors="replace").strip()
            return value or None
        except (OSError, asyncio.TimeoutError):
            return None

    async def _pid_matches_record(self, record: dict[str, Any]) -> bool:
        try:
            pid = int(record.get("pid", 0))
            task_json = str(record["task_json"])
        except (KeyError, TypeError, ValueError):
            return False
        work_dir = Path(task_json).parent
        expected = self._launch_expression(Path(task_json), work_dir)
        if record.get("command_sha256") != self._command_fingerprint(expected):
            return False
        command_line = await self._pid_command_line(pid)
        normalized_expected = expected.replace("/", "\\").lower()
        normalized_command = (command_line or "").replace("/", "\\").lower()
        return "-batch" in normalized_command and normalized_expected in normalized_command

    def _record_matches_running(self, record: dict[str, Any], batch_id: str,
                                payload: dict[str, Any], work_dir: Path) -> bool:
        try:
            return (
                str(record["batch_attempt_id"]) == batch_id
                and str(record["lease_token"]) == str(payload["lease_token"])
                and Path(str(record["task_json"])).resolve() == (work_dir / "task.json").resolve()
                and int(record["pid"]) > 0
            )
        except (KeyError, TypeError, ValueError):
            return False

    def schedule_pool_capacity_probe(self) -> None:
        """Probe an unavailable profile in the background without delaying heartbeats."""
        if self.configured_pool_workers > 0 or time.monotonic() < self.next_pool_probe_at:
            return
        if self.pool_probe_task is not None and not self.pool_probe_task.done():
            return
        self.next_pool_probe_at = time.monotonic() + 30
        self.pool_probe_task = asyncio.create_task(self.probe_pool_capacity())

    async def probe_pool_capacity(self) -> bool:
        """Read the configured MATLAB profile without creating a parpool."""
        profile = _safe_name(str(self.config.get("cluster_profile", "local")), "cluster profile")
        matlab = str(self.config.get("matlab_exe", "matlab"))
        marker = "PLATEMO_HPC_POOL_WORKERS="
        expression = f"p=parcluster('{_matlab_quote(profile)}');fprintf('{marker}%d\\n',p.NumWorkers);exit"
        try:
            probe_timeout = min(600, max(45, int(self.config.get("profile_probe_timeout_seconds", 300))))
        except (TypeError, ValueError):
            probe_timeout = 300
        process: asyncio.subprocess.Process | None = None
        failure = "profile probe produced no capacity marker"
        try:
            process = await asyncio.create_subprocess_exec(
                matlab, "-batch", expression, cwd=str(self.root),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            )
            self.pool_probe_process = process
            # A cold Windows MATLAB start can take several minutes.  The probe
            # runs in the background, so it must not be cut off by the normal
            # heartbeat interval.
            output, _ = await asyncio.wait_for(process.communicate(), timeout=probe_timeout)
            for line in (output or b"").decode(errors="replace").splitlines():
                if line.startswith(marker):
                    self.configured_pool_workers = max(0, int(line.removeprefix(marker).strip()))
                    if self.configured_pool_workers > 0:
                        if self.pool_probe_error:
                            self.log.info("MATLAB profile is ready profile=%s workers=%s", profile, self.configured_pool_workers)
                        self.pool_probe_error = ""
                        return True
                    failure = "profile reports zero workers"
                    break
        except (OSError, ValueError, asyncio.TimeoutError) as exc:
            failure = str(exc) or type(exc).__name__
            if process is not None and process.returncode is None:
                await self._terminate_process(process)
        except asyncio.CancelledError:
            if process is not None and process.returncode is None:
                await self._terminate_process(process)
            raise
        finally:
            if self.pool_probe_process is process:
                self.pool_probe_process = None
        self.configured_pool_workers = 0
        if failure != self.pool_probe_error:
            self.log.warning("MATLAB profile is not ready profile=%s: %s; retrying", profile, failure)
        self.pool_probe_error = failure
        return False

    async def stop_pool_capacity_probe(self) -> None:
        """Cancel a startup probe so shutdown never waits for cold MATLAB."""
        task = self.pool_probe_task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        process = self.pool_probe_process
        if process is not None and process.returncode is None:
            await self._terminate_process(process)

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
            await self.prepare_assignment_settings(payload)
            self.validate_environment(payload)
            payload["cluster_profile"] = local_profile
            batch_id = str(uuid.UUID(batch_id))
            point_id = str(uuid.UUID(point_id))
            if not str(payload.get("lease_token", "")):
                raise HTTPException(422, "lease_token is required")
        except ValueError as exc:
            raise HTTPException(400, "batch_attempt_id and experiment_point_id must be UUIDs") from exc
        target = self.queue / f"{batch_id}.json"
        if target.exists() or (self.running / target.name).exists():
            self.log.info("assignment already recorded batch=%s", batch_id)
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
        self.log.info("assignment accepted batch=%s point=%s seeds=%d profile=%s", batch_id, point_id, len(seeds), local_profile)
        if self.config.get("auto_run"):
            self.start_task(batch_id)
        return {"status": "accepted", "batch_attempt_id": batch_id}

    def start_task(self, batch_id: str) -> None:
        if batch_id not in self.tasks:
            self.log.info("batch queued for execution batch=%s", batch_id)
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
            self.log.info("batch execution started batch=%s point=%s seeds=%d", batch_id, point_id, len(payload["seeds"]))
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
                self.log.warning("batch did not start because initial progress was not accepted batch=%s", batch_id)
                running.unlink(missing_ok=True)
                return
            if batch_id in self.invalidated_batches:
                self.log.warning("batch invalidated before MATLAB start batch=%s", batch_id)
                running.unlink(missing_ok=True)
                return
            expression = self._launch_expression(task_json, work_dir)
            executable = str(self.config.get("matlab_exe", "matlab"))
            process = await asyncio.create_subprocess_exec(
                executable, "-batch", expression, cwd=str(self.root),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            )
            self.processes[batch_id] = process
            self.batches[batch_id]["matlab_pid"] = process.pid
            self.batches[batch_id]["pool"] = {"active": True, "workers": 0,
                                               "cluster_profile": payload["cluster_profile"]}
            self._write_process_record(work_dir, batch_id, lease_token, process, expression)
            self.log.info("MATLAB started batch=%s pid=%s profile=%s", batch_id, process.pid, payload["cluster_profile"])
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
            # A progress request may be blocked in a transient network failure.
            # Do not let its cancellation hold the completed MATLAB batch hostage:
            # the local result is already durable and must enter delivery/retry.
            progress_task.add_done_callback(self._log_progress_task_failure)
            seed_files = [work_dir / "runs" / f"seed-{seed}.mat" for seed in payload["seeds"]]
            all_seed_files = bool(seed_files) and all(path.is_file() and path.stat().st_size > 0 for path in seed_files)
            state = "completed" if code == 0 and all_seed_files else "failed"
            if batch_id in self.cancelled or payload.get("cancel_requested"):
                state = "cancelled"
            level = logging.INFO if state == "completed" else logging.WARNING
            self.log.log(level, "MATLAB finished batch=%s state=%s exit_code=%s seed_files=%s", batch_id, state, code, sum(path.is_file() for path in seed_files))
            summary = {"experiment_point_id": point_id, "batch_attempt_id": batch_id, "state": state,
                       "exit_code": code, "result": None, "sha256": None, "finished_at": now()}
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
            delivery_path = work_dir / "delivery.json"
            if not delivery_path.is_file():
                self.write_delivery(delivery_path, {"seed_artifacts": {},
                                                   "complete_confirmed": False, "updated_at": now()})
            if batch_id in self.invalidated_batches:
                running.unlink(missing_ok=True)
                return
            if await self.deliver_batch(point_id, batch_id, lease_token, payload["seeds"], summary, work_dir):
                running.unlink(missing_ok=True)
        except Exception as exc:
            self.log.exception("batch execution failed batch=%s", batch_id)
            error_dir = work_dir or (self.work / batch_id)
            error_dir.mkdir(parents=True, exist_ok=True)
            failure = {"experiment_point_id": point_id, "batch_attempt_id": batch_id,
                       "state": "failed", "error": str(exc), "finished_at": now()}
            (error_dir / "worker-error.json").write_text(json.dumps(failure, indent=2), encoding="utf-8")
            if point_id and lease_token and batch_id not in self.invalidated_batches:
                if await self.deliver_batch(point_id, batch_id, lease_token,
                                            list(payload.get("seeds", [])), failure, error_dir):
                    running.unlink(missing_ok=True)
        finally:
            self.processes.pop(batch_id, None)
            self.tasks.pop(batch_id, None)
            self.batches.pop(batch_id, None)
            if work_dir is not None:
                self._process_record_path(work_dir).unlink(missing_ok=True)
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

    def _log_progress_task_failure(self, task: asyncio.Task[None]) -> None:
        """Consume an abandoned reporter exception without blocking batch cleanup."""
        if task.cancelled():
            return
        try:
            task.result()
        except Exception:
            self.log.warning("progress reporter stopped after cancellation", exc_info=True)

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
            batch = self.batches.get(batch_id)
            if batch is not None:
                # Master persists the PID from progress, so every update must
                # carry it; delivery explicitly reports zero.
                progress["matlab_pid"] = int(batch.get("matlab_pid", 0) or 0)
                progress["phase"] = str(batch.get("phase", progress.get("phase", "running")))
                runs = progress.get("runs", [])
                if isinstance(runs, list):
                    heartbeat_runs: list[dict[str, Any]] = []
                    for run in runs:
                        if not isinstance(run, dict) or not isinstance(run.get("seed"), int):
                            continue
                        snapshot = dict(run)
                        try:
                            fe = max(0, int(snapshot.get("fe", 0) or 0))
                            total_fe = max(0, int(snapshot.get("total_fe", 0) or 0))
                            elapsed = max(0.0, float(snapshot.get("elapsed_seconds", 0) or 0))
                        except (TypeError, ValueError):
                            continue
                        snapshot["fe"] = fe
                        snapshot["total_fe"] = total_fe
                        snapshot["elapsed_seconds"] = elapsed
                        snapshot["eta_seconds"] = (elapsed * (total_fe - fe) / fe
                                                   if fe > 0 and total_fe > fe else 0.0)
                        heartbeat_runs.append(snapshot)
                    # Keep a complete, normalized Seed summary so a heartbeat
                    # can restore the UI state when a progress request is lost.
                    batch["runs"] = heartbeat_runs
                    batch["completed_runs"] = int(progress.get("completed_runs", 0) or 0)
                    batch["failed_runs"] = int(progress.get("failed_runs", 0) or 0)
                    batch["running_runs"] = int(progress.get("running_runs", 0) or 0)
                    batch["total_runs"] = int(progress.get("total_runs", len(heartbeat_runs)) or 0)
                    batch["progress_timestamp"] = str(progress.get("timestamp", "") or "")
            pool = progress.get("pool", {})
            if isinstance(pool, dict):
                workers = pool.get("workers", 0)
                if isinstance(workers, int) and workers >= 0:
                    self.actual_pool_workers = workers
                    if batch is not None:
                        batch["actual_pool_workers"] = workers
                        batch["pool"] = pool
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

    def write_delivery(self, path: Path, delivery: dict[str, Any]) -> None:
        path.write_text(json.dumps(delivery, indent=2), encoding="utf-8")

    async def deliver_batch(self, point_id: str, batch_id: str, lease_token: str,
                            seeds: list[int], summary: dict[str, Any], work_dir: Path) -> bool:
        delivery_path = work_dir / "delivery.json"
        try:
            delivery = json.loads(delivery_path.read_text(encoding="utf-8")) if delivery_path.is_file() else {}
        except (OSError, json.JSONDecodeError):
            delivery = {}
        delivery.setdefault("seed_artifacts", {})
        delivery.setdefault("complete_confirmed", False)
        delivery["updated_at"] = now()
        self.write_delivery(delivery_path, delivery)
        self.log.info("delivery started batch=%s seed_artifacts=%d", batch_id, len(delivery["seed_artifacts"]))
        self.actual_pool_workers = 0
        batch = self.batches.get(batch_id)
        if batch is not None:
            batch["matlab_pid"] = 0
            batch["actual_pool_workers"] = 0
            batch["pool"] = {"active": False, "workers": 0, "phase": "delivering"}
            batch["phase"] = "delivering"
        progress_path = work_dir / "progress.json"
        progress = {
            "phase": "delivering", "runs": summary.get("runs", []),
            "completed_runs": summary.get("completed_runs", 0), "failed_runs": summary.get("failed_runs", 0),
            "running_runs": 0, "total_runs": summary.get("total_runs", len(seeds)),
            "pool": {"active": False, "workers": 0, "phase": "delivering"}, "timestamp": now(),
        }
        progress_path.write_text(json.dumps(progress), encoding="utf-8")
        while batch_id not in self.invalidated_batches:
            runs = {int(run["seed"]): run for run in summary.get("runs", [])
                    if isinstance(run, dict) and isinstance(run.get("seed"), int)}
            required_seed_artifacts: list[str] = []
            missing_seed_artifact = False
            for seed in seeds:
                run = runs.get(seed, {})
                if run.get("state") != "completed":
                    continue
                required_seed_artifacts.append(str(seed))
                seed_path = work_dir / "runs" / f"seed-{seed}.mat"
                if not seed_path.is_file():
                    missing_seed_artifact = True
                    self.log.warning("completed Seed artifact is missing batch=%s seed=%s", batch_id, seed)
                    continue
                entry = delivery["seed_artifacts"].setdefault(str(seed), {"artifact_id": str(uuid.uuid4()), "uploaded": False})
                if entry.get("uploaded"):
                    continue
                uploaded = await self.upload_artifact(point_id, batch_id, lease_token, seed_path,
                                                       [seed], str(entry["artifact_id"]),
                                                       kind="seed_result")
                if uploaded is True:
                    entry["uploaded"] = True
                    delivery["updated_at"] = now()
                    self.write_delivery(delivery_path, delivery)
                    self.log.info("seed artifact uploaded batch=%s seed=%s artifact=%s", batch_id, seed, entry["artifact_id"])
                elif uploaded is False:
                    self.log.warning("delivery invalidated during artifact upload batch=%s", batch_id)
                    return False
            all_seed_artifacts_uploaded = not missing_seed_artifact and all(
                delivery["seed_artifacts"].get(seed, {}).get("uploaded", False)
                for seed in required_seed_artifacts
            )
            if all_seed_artifacts_uploaded:
                completed = await self.complete_master(batch_id, lease_token, summary, work_dir)
                if completed is True:
                    delivery["complete_confirmed"] = True
                    delivery["updated_at"] = now()
                    self.write_delivery(delivery_path, delivery)
                    self.log.info("delivery confirmed batch=%s", batch_id)
                    return True
                if completed is False:
                    self.log.warning("delivery invalidated during completion batch=%s", batch_id)
                    return False
            # Retain the slot and its local files until delivery is confirmed.
            # A successful progress update renews the lease during transient outages.
            if await self.send_progress(batch_id, lease_token, work_dir) is False:
                return False
            await asyncio.sleep(5)
        return False

    async def resume_delivery(self, batch_id: str, payload: dict[str, Any]) -> None:
        """Retry delivery of an already-finished batch without restarting MATLAB."""
        point_id = str(payload.get("experiment_point_id", ""))
        lease_token = str(payload.get("lease_token", ""))
        work_dir = self.work / point_id / batch_id
        summary_path = work_dir / "summary.json"
        if not point_id or not lease_token or not summary_path.is_file():
            return
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        self.batches[batch_id] = {"experiment_point_id": point_id, "lease_token": lease_token,
                                  "seeds": payload.get("seeds", []), "matlab_pid": 0}
        try:
            delivered = await self.deliver_batch(point_id, batch_id, lease_token,
                                                 list(payload.get("seeds", [])), summary, work_dir)
            if delivered:
                (self.running / f"{batch_id}.json").unlink(missing_ok=True)
        finally:
            self.batches.pop(batch_id, None)
            self.tasks.pop(batch_id, None)

    async def resume_deliveries(self) -> None:
        """Recover only pending network delivery, never a persisted MATLAB assignment."""
        for path in list(self.running.glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                batch_id = str(uuid.UUID(path.stem))
                point_id = str(payload.get("experiment_point_id", ""))
                work_dir = self.work / point_id / batch_id
                if (work_dir / "summary.json").is_file():
                    self.tasks[batch_id] = asyncio.create_task(self.resume_delivery(batch_id, payload))
            except (OSError, ValueError, json.JSONDecodeError):
                continue

    async def monitor_recovered_process(self, batch_id: str, work_dir: Path, record: dict[str, Any]) -> None:
        """Keep a verified recovered MATLAB lease and its Seed progress visible."""
        payload: dict[str, Any] = {}
        try:
            while await self._pid_matches_record(record):
                if await self.send_progress(batch_id, str(record["lease_token"]), work_dir) is False:
                    return
                await asyncio.sleep(2)
            running_path = self.running / f"{batch_id}.json"
            if not running_path.is_file() or batch_id in self.invalidated_batches:
                return
            payload = json.loads(running_path.read_text(encoding="utf-8"))
            point_id = str(payload["experiment_point_id"])
            seeds = list(payload.get("seeds", []))
            seed_files = [work_dir / "runs" / f"seed-{seed}.mat" for seed in seeds]
            state = "cancelled" if batch_id in self.cancelled else (
                "completed" if seed_files and all(path.is_file() and path.stat().st_size for path in seed_files) else "failed"
            )
            progress: dict[str, Any] = {}
            progress_path = work_dir / "progress.json"
            if progress_path.is_file():
                progress = json.loads(progress_path.read_text(encoding="utf-8"))
            summary = {"experiment_point_id": point_id, "batch_attempt_id": batch_id, "state": state,
                       "exit_code": None, "result": None, "sha256": None, "finished_at": now()}
            for key in ("runs", "completed_runs", "failed_runs", "running_runs", "total_runs", "pool"):
                if key in progress:
                    summary[key] = progress[key]
            if state == "completed" and not summary.get("runs"):
                seeds = list(payload.get("seeds", []))
                summary["runs"] = [{"seed": seed, "state": "completed", "fe": payload.get("max_fe", 0),
                                    "total_fe": payload.get("max_fe", 0), "elapsed_seconds": 0, "error": ""}
                                   for seed in seeds]
                summary["completed_runs"], summary["failed_runs"], summary["running_runs"], summary["total_runs"] = len(seeds), 0, 0, len(seeds)
            if state == "cancelled":
                self._mark_unfinished_cancelled(summary, list(payload.get("seeds", [])), int(payload.get("max_fe", 0)))
            (work_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
            delivery_path = work_dir / "delivery.json"
            if not delivery_path.is_file():
                self.write_delivery(delivery_path, {"seed_artifacts": {},
                                                   "complete_confirmed": False, "updated_at": now()})
            if await self.deliver_batch(point_id, batch_id, str(record["lease_token"]),
                                        list(payload.get("seeds", [])), summary, work_dir):
                running_path.unlink(missing_ok=True)
        finally:
            if batch_id not in self.invalidated_batches:
                try:
                    running_path = self.running / f"{batch_id}.json"
                    payload = json.loads(running_path.read_text(encoding="utf-8"))
                    summary_path = work_dir / "summary.json"
                    if summary_path.is_file():
                        summary = json.loads(summary_path.read_text(encoding="utf-8"))
                    else:
                        state = "cancelled" if batch_id in self.cancelled else (
                            "completed" if all((work_dir / "runs" / f"seed-{seed}.mat").is_file()
                                               for seed in payload.get("seeds", [])) else "failed"
                        )
                        summary = build_summary(payload, work_dir, state, None)
                        if state == "cancelled":
                            self._mark_unfinished_cancelled(summary, list(payload.get("seeds", [])), int(payload.get("max_fe", 0)))
                        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
                    delivered = await self.deliver_batch(
                        str(payload["experiment_point_id"]), batch_id, str(payload["lease_token"]),
                        list(payload.get("seeds", [])), summary, work_dir,
                    )
                    if delivered:
                        running_path.unlink(missing_ok=True)
                except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
                    pass
            self.batches.pop(batch_id, None)
            self.tasks.pop(batch_id, None)
            self.orphaned_pids.pop(batch_id, None)
            self._process_record_path(work_dir).unlink(missing_ok=True)
            self.actual_pool_workers = 0

    async def terminate_pid_tree(self, pid: int) -> None:
        if pid < 1:
            return
        if sys.platform == "win32":
            killer = await asyncio.create_subprocess_exec(
                "taskkill", "/PID", str(pid), "/T", "/F",
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            )
            await killer.wait()

    async def recover_running_processes(self) -> None:
        """Restore only positively identified MATLAB processes after a Worker restart."""
        for path in list(self.running.glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                batch_id = str(uuid.UUID(path.stem))
                point_id = str(payload["experiment_point_id"])
                work_dir = self.work / point_id / batch_id
                record_path = self._process_record_path(work_dir)
                if not record_path.is_file() or (work_dir / "summary.json").is_file():
                    continue
                record = json.loads(record_path.read_text(encoding="utf-8"))
                pid = int(record.get("pid", 0))
                if self._record_matches_running(record, batch_id, payload, work_dir) and await self._pid_matches_record(record):
                    self.batches[batch_id] = {
                        "experiment_point_id": point_id, "lease_token": str(payload.get("lease_token", "")),
                        "seeds": payload.get("seeds", []), "matlab_pid": pid,
                        "actual_pool_workers": 0, "pool": {"active": True, "workers": 0, "phase": "recovered"},
                        "phase": "recovered",
                    }
                    self.orphaned_pids[batch_id] = pid
                    self.tasks[batch_id] = asyncio.create_task(self.monitor_recovered_process(batch_id, work_dir, record))
                else:
                    await self.terminate_pid_tree(pid)
                    seeds = list(payload.get("seeds", []))
                    seed_files = [work_dir / "runs" / f"seed-{seed}.mat" for seed in seeds]
                    if seed_files and all(path.is_file() and path.stat().st_size > 0 for path in seed_files):
                        # The worker can be interrupted after MATLAB has finished
                        # but before summary.json is persisted. Preserve those
                        # completed results for the normal delivery retry path.
                        summary = build_summary(payload, work_dir, "completed", 0)
                        (work_dir / "summary.json").write_text(
                            json.dumps(summary, indent=2), encoding="utf-8"
                        )
                        delivery_path = work_dir / "delivery.json"
                        if not delivery_path.is_file():
                            self.write_delivery(delivery_path, {
                                "seed_artifacts": {}, "complete_confirmed": False, "updated_at": now(),
                            })
                        self.log.info("recovered completed MATLAB output batch=%s; resuming delivery", batch_id)
            except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
                continue

    async def upload_artifact(self, point_id: str, batch_id: str, lease_token: str, artifact_path: Path,
                              seeds: list[int], artifact_id: str, *, kind: str) -> bool | None:
        master_url = str(self.config.get("master_url", "")).rstrip("/")
        if batch_id in self.invalidated_batches or not master_url or not self.node_token:
            return
        try:
            fields = {
                "experiment_point_id": point_id,
                "batch_attempt_id": batch_id,
                "lease_token": lease_token,
                "kind": kind,
            }
            if len(seeds) == 1:
                fields["seed"] = str(seeds[0])
            async with httpx.AsyncClient(timeout=120) as client:
                with artifact_path.open("rb") as handle:
                    response = await client.put(
                        f"{master_url}/api/v1/artifacts/{artifact_id}",
                        data=fields,
                        files={"artifact": (artifact_path.name, handle, "application/octet-stream")},
                        headers={"Authorization": f"Bearer {self.node_token}"},
                    )
                    response.raise_for_status()
            return True
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 410:
                await self.invalidate_batch(batch_id)
                return False
            self.log.warning("artifact upload failed batch=%s status=%s; retrying", batch_id, exc.response.status_code)
            (artifact_path.parent / "upload-error.txt").write_text(str(exc), encoding="utf-8")
        except httpx.HTTPError as exc:
            self.log.warning("artifact upload failed batch=%s; retrying: %s", batch_id, exc)
            (artifact_path.parent / "upload-error.txt").write_text(str(exc), encoding="utf-8")
        return None

    async def complete_master(self, batch_id: str, lease_token: str, summary: dict[str, Any],
                              work_dir: Path | None = None) -> bool | None:
        master_url = str(self.config.get("master_url", "")).rstrip("/")
        if batch_id in self.invalidated_batches or not master_url or not self.node_token:
            return False if batch_id in self.invalidated_batches else None
        payload = dict(summary)
        payload["lease_token"] = lease_token
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(f"{master_url}/api/v1/batch-attempts/{batch_id}/complete", json=payload, headers={"Authorization": f"Bearer {self.node_token}"})
                response.raise_for_status()
            return True
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 410:
                await self.invalidate_batch(batch_id)
                return False
            self.log.warning("completion upload failed batch=%s status=%s; retrying", batch_id, exc.response.status_code)
            error_path = (work_dir or self.work / batch_id) / "complete-upload-error.txt"
            error_path.parent.mkdir(parents=True, exist_ok=True)
            error_path.write_text(str(exc), encoding="utf-8")
        except httpx.HTTPError as exc:
            self.log.warning("completion upload failed batch=%s; retrying: %s", batch_id, exc)
            error_path = (work_dir or self.work / batch_id) / "complete-upload-error.txt"
            error_path.parent.mkdir(parents=True, exist_ok=True)
            error_path.write_text(str(exc), encoding="utf-8")
        return None

    async def invalidate_batch(self, batch_id: str) -> None:
        """Stop an expired lease and leave its files intact for Master recovery."""
        if batch_id in self.invalidated_batches:
            return
        self.invalidated_batches.add(batch_id)
        process = self.processes.get(batch_id)
        if process and process.returncode is None:
            await self._terminate_process(process)
            return
        orphan_pid = self.orphaned_pids.get(batch_id)
        if orphan_pid:
            await self.terminate_pid_tree(orphan_pid)

    @staticmethod
    def _mark_unfinished_cancelled(summary: dict[str, Any], seeds: list[int], max_fe: int) -> None:
        mark_unfinished_cancelled(summary, seeds, max_fe)

    async def reject_assignment(self, assignment: dict[str, Any], code: str, message: str) -> None:
        master_url = str(self.config.get("master_url", "")).rstrip("/")
        batch_id = str(assignment.get("batch_attempt_id", ""))
        lease_token = str(assignment.get("lease_token", ""))
        if not master_url or not self.node_token or not batch_id or not lease_token:
            return
        payload = {"lease_token": lease_token, "phase": "rejected", "error_code": code,
                   "error": message, "runs": []}
        self.log.warning("assignment rejected batch=%s code=%s detail=%s", batch_id, code, message)
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
            response = await client.post(f"{master_url}/api/v2/workers/register", json=payload, headers={"Authorization": f"Bearer {join_token}"})
            response.raise_for_status()
            self.node_token = response.json()["node_token"]
            self.save_config()
            self.log.info("registered with Master worker_id=%s", self.worker_id)

    async def control_loop(self) -> None:
        while True:
            try:
                self.schedule_pool_capacity_probe()
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
                        self.set_master_connection(True)
                        for batch_id in response.get("cancel_batch_attempt_ids", []):
                            await self.cancel(str(batch_id))
                        assignment = response.get("assignment")
                        if assignment and self.can_accept_assignment():
                            try:
                                await self.enqueue(dict(assignment))
                            except HTTPException as exc:
                                error_code = str(exc.detail)
                                if error_code not in {"profile_unavailable", "input_incompatible", "version_mismatch", "insufficient_disk"}:
                                    error_code = "input_incompatible"
                                await self.reject_assignment(dict(assignment), error_code, str(exc.detail))
            except httpx.HTTPStatusError as exc:
                self.set_master_connection(False, f"HTTP {exc.response.status_code}")
                if exc.response.status_code == 401:
                    self.node_token = ""
                    self.save_config()
            except (httpx.HTTPError, OSError, HTTPException) as exc:
                self.set_master_connection(False, str(exc))
            await asyncio.sleep(10)

    async def cancel(self, batch_id: str) -> bool:
        process = self.processes.get(batch_id)
        if process and process.returncode is None:
            self.cancelled.add(batch_id)
            self.log.warning("cancelling MATLAB batch=%s pid=%s", batch_id, process.pid)
            await self._terminate_process(process)
            return True
        orphan_pid = self.orphaned_pids.get(batch_id)
        if orphan_pid:
            self.cancelled.add(batch_id)
            self.log.warning("cancelling recovered MATLAB batch=%s pid=%s", batch_id, orphan_pid)
            await self.terminate_pid_tree(orphan_pid)
            return True
        path = self.queue / f"{batch_id}.json"
        if path.exists():
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["state"] = "cancelled"
            point_id = str(payload.get("experiment_point_id", ""))
            work_dir = self.work / point_id / batch_id
            work_dir.mkdir(parents=True, exist_ok=True)
            summary = {
                "experiment_point_id": str(payload.get("experiment_point_id", "")),
                "batch_attempt_id": batch_id,
                "state": "cancelled",
                "exit_code": None,
                "runs": [{"seed": seed, "state": "cancelled", "fe": 0,
                          "total_fe": payload.get("max_fe", 0), "elapsed_seconds": 0,
                          "error": "cancelled before MATLAB start"} for seed in payload.get("seeds", [])],
                "finished_at": now(),
            }
            (work_dir / "task.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
            (work_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
            path.replace(self.running / path.name)
            self.tasks[batch_id] = asyncio.create_task(self.resume_delivery(batch_id, payload))
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
