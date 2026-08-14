"""Persistent MATLAB pool controller for dynamic, cross-point Seed scheduling."""
from __future__ import annotations

import asyncio
import json
import logging
import hashlib
import time
import uuid
from pathlib import Path
from typing import Any

import httpx


def _quote(value: Path | str) -> str:
    return str(value).replace("'", "''")


class DynamicSession:
    """Own one MATLAB Supervisor; file queues isolate MATLAB from network I/O."""

    def __init__(self, state: Any) -> None:
        self.state = state
        self.root = state.root
        self.data = state.data / "dynamic-session"
        self.inbox = self.data / "inbox"
        self.outbox = self.data / "outbox"
        self.runs = self.data / "runs"
        for path in (self.inbox, self.outbox, self.runs):
            path.mkdir(parents=True, exist_ok=True)
        self.session_id = str(uuid.uuid4())
        self.process: asyncio.subprocess.Process | None = None
        self.running: dict[str, dict[str, Any]] = {}
        self.queued_delivery_paths: set[Path] = set()
        self.upload_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.upload_task: asyncio.Task[None] | None = None
        self.process_log_handle: Any | None = None
        self.next_start_at = 0.0
        self.start_backoff_seconds = 5.0
        self.log: logging.Logger = state.log

    @property
    def _process_record(self) -> Path:
        return self.data / "supervisor-process.json"

    @staticmethod
    def _fingerprint(expression: str) -> str:
        return hashlib.sha256(expression.encode("utf-8")).hexdigest()

    async def recover_previous_supervisor(self) -> None:
        """A detached old Supervisor cannot be safely reattached, so stop only
        the process whose command line proves it is our persisted session."""
        if not self._process_record.is_file():
            return
        try:
            record = json.loads(self._process_record.read_text(encoding="utf-8"))
            pid = int(record["pid"])
        except (OSError, KeyError, ValueError, json.JSONDecodeError):
            self._process_record.unlink(missing_ok=True)
            return
        command = await self.state._pid_command_line(pid)
        expected = str(record.get("command_sha256", ""))
        expression = str(record.get("expression", ""))
        normalized = (command or "").replace("/", "\\").lower()
        if (expected == self._fingerprint(expression) and "-batch" in normalized
                and "run_dynamic_session(" in normalized
                and str(self.inbox).replace("/", "\\").lower() in normalized):
            self.log.warning("terminating recovered dynamic MATLAB supervisor pid=%s", pid)
            await self.state.terminate_pid_tree(pid)
        self._process_record.unlink(missing_ok=True)
        (self.data / "session.json").unlink(missing_ok=True)

    @property
    def actual_workers(self) -> int:
        state_path = self.data / "session.json"
        if not self.process or self.process.returncode is not None or not state_path.is_file():
            return 0
        try:
            return max(0, int(json.loads(state_path.read_text(encoding="utf-8")).get("actual_workers", 0)))
        except (OSError, ValueError, json.JSONDecodeError):
            return 0

    async def start(self) -> None:
        if self.process and self.process.returncode is None:
            return
        if time.monotonic() < self.next_start_at:
            return
        if self.state.configured_pool_workers < 1:
            return
        if self.process and self.process.returncode is not None and self.process_log_handle:
            self.process_log_handle.close()
            self.process_log_handle = None
        worker_dir = Path(__file__).resolve().parent.parent
        (self.data / "session.json").unlink(missing_ok=True)
        expression = f"addpath('{_quote(worker_dir)}');run_dynamic_session('{_quote(self.inbox)}','{_quote(self.outbox)}','{_quote(self.runs)}','{_quote(self.state.config.get('cluster_profile','local'))}')"
        if self.process and self.process.returncode is not None:
            self.log.warning("dynamic MATLAB session exited code=%s; retry in %.0fs", self.process.returncode, self.start_backoff_seconds)
        log_path = self.data / "logs"
        log_path.mkdir(parents=True, exist_ok=True)
        self.process_log_handle = (log_path / "matlab-supervisor.log").open("a", encoding="utf-8")
        try:
            self.process = await asyncio.create_subprocess_exec(
                str(self.state.config.get("matlab_exe", "matlab")), "-batch", expression,
                cwd=str(self.root), stdout=self.process_log_handle, stderr=asyncio.subprocess.STDOUT,
            )
        except OSError:
            self.process_log_handle.close()
            self.process_log_handle = None
            self.next_start_at = time.monotonic() + self.start_backoff_seconds
            self.start_backoff_seconds = min(60.0, self.start_backoff_seconds * 2)
            raise
        self._process_record.write_text(json.dumps({"pid": self.process.pid, "expression": expression,
                                                     "command_sha256": self._fingerprint(expression)}, indent=2),
                                        encoding="utf-8")
        self.next_start_at = time.monotonic() + self.start_backoff_seconds
        self.start_backoff_seconds = 5.0
        self.log.info("dynamic MATLAB session started id=%s pid=%s", self.session_id, self.process.pid)

    async def stop(self) -> None:
        if self.upload_task:
            self.upload_task.cancel()
        if self.process and self.process.returncode is None:
            await self.state._terminate_process(self.process)
        if self.process_log_handle:
            self.process_log_handle.close()
            self.process_log_handle = None
        self._process_record.unlink(missing_ok=True)

    def _free_slots(self) -> int:
        computing = sum(1 for item in self.running.values() if item.get("phase", "running") != "delivering")
        return max(0, self.actual_workers - computing)

    async def run(self) -> None:
        if self.upload_task is None or self.upload_task.done():
            self.upload_task = asyncio.create_task(self._upload_loop())
        while True:
            self.state.schedule_pool_capacity_probe()
            try:
                # Dynamic mode still uses the common join-token registration
                # path when a node token has not been persisted yet.
                await self.state.register_with_master()
                await self.start()
            except OSError as exc:
                self.log.error("unable to start dynamic MATLAB session: %s", exc)
                await asyncio.sleep(2)
                continue
            await self._consume_outbox()
            await self._heartbeat()
            await asyncio.sleep(2)

    async def _heartbeat(self) -> None:
        if not self.state.node_token:
            return
        supervisor_alive = bool(self.process and self.process.returncode is None)
        if not supervisor_alive and not self.running:
            return
        payload = {
            "session_id": self.session_id,
            "pool": {"state": "ready" if self.actual_workers > 0 else ("starting" if supervisor_alive else "recovering"), "configured_workers": self.state.configured_pool_workers,
                     "actual_workers": self.actual_workers},
            "free_seed_slots": self._free_slots() if self.actual_workers > 0 else 0,
            "running_seeds": list(self.running.values()),
        }
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(f"{self.state.config['master_url'].rstrip('/')}/api/v2/workers/{self.state.worker_id}/heartbeat", json=payload, headers={"Authorization": f"Bearer {self.state.node_token}"})
                response.raise_for_status()
            self.state.set_master_connection(True)
            for attempt_id in response.json().get("cancel_attempt_ids", []):
                await self._cancel(str(attempt_id))
            for assignment in response.json().get("assignments", []):
                await self._accept(dict(assignment))
        except httpx.HTTPStatusError as exc:
            self.state.set_master_connection(False, f"HTTP {exc.response.status_code}")
            if exc.response.status_code == 401:
                self.state.node_token = ""
                self.state.save_config()
        except httpx.HTTPError as exc:
            self.state.set_master_connection(False, str(exc))

    async def _accept(self, assignment: dict[str, Any]) -> None:
        attempt_id = str(assignment["attempt_id"])
        if attempt_id in self.running:
            return
        try:
            await self.state.prepare_assignment_settings(assignment)
            self.state.validate_environment(assignment)
        except Exception as exc:
            self.log.warning("dynamic Seed rejected attempt=%s: %s", attempt_id, exc)
            await self._reject(assignment, str(exc))
            return
        assignment["run_dir"] = str(self.runs / attempt_id)
        target = self.inbox / f"{attempt_id}.json"
        temporary = target.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(assignment), encoding="utf-8")
        temporary.replace(target)
        self.running[attempt_id] = {"attempt_id": attempt_id, "lease_token": assignment["lease_token"],
                                    "experiment_point_id": assignment["experiment_point_id"], "seed": assignment["seed"],
                                    "fe": 0, "total_fe": assignment["max_fe"], "elapsed_seconds": 0,
                                    "phase": "running"}
        self.log.info("dynamic Seed accepted attempt=%s point=%s seed=%s", attempt_id,
                      assignment["experiment_point_id"], assignment["seed"])

    async def _reject(self, assignment: dict[str, Any], reason: str) -> None:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(
                    f"{self.state.config['master_url'].rstrip('/')}/api/v2/seed-attempts/{assignment['attempt_id']}/reject",
                    json={"lease_token": assignment["lease_token"], "reason": reason[:1000]},
                    headers={"Authorization": f"Bearer {self.state.node_token}"},
                )
                response.raise_for_status()
        except httpx.HTTPError as exc:
            self.log.warning("dynamic Seed reject report failed attempt=%s: %s", assignment["attempt_id"], exc)

    async def _cancel(self, attempt_id: str) -> None:
        item = self.running.get(attempt_id)
        if item is None:
            return
        if item.get("phase") == "delivering":
            # MATLAB has already completed. Preserve the completed artifact
            # delivery; Master decides its final cancellation race policy.
            return
        path = self.data / "cancel" / f"{attempt_id}.cancel"
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.is_file():
            path.write_text("cancelled by Master", encoding="utf-8")
            self.log.info("dynamic Seed cancellation forwarded attempt=%s", attempt_id)

    async def _consume_outbox(self) -> None:
        for path in list(self.outbox.glob("*.json")) + list(self.outbox.glob("*.delivering")):
            if path in self.queued_delivery_paths:
                continue
            try:
                event = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            attempt_id = str(event.get("attempt_id", ""))
            item = self.running.get(attempt_id)
            if item is not None:
                item.update({key: event[key] for key in ("fe", "total_fe", "elapsed_seconds") if key in event})
            state = str(event.get("state", "running"))
            if state in {"completed", "failed", "cancelled"}:
                if item is not None:
                    event.update(item)
                    item["phase"] = "delivering"
                    event["phase"] = "delivering"
                else:
                    # Outbox is durable. Reconstruct delivery ownership after a
                    # Worker restart without waiting for a fresh MATLAB pool.
                    item = {key: event.get(key) for key in ("attempt_id", "lease_token", "experiment_point_id", "seed", "fe", "total_fe", "elapsed_seconds")}
                    item["phase"] = "delivering"
                    self.running[attempt_id] = item
                    event["phase"] = "delivering"
                event["_event_path"] = str(path)
                self.queued_delivery_paths.add(path)
                await self.upload_queue.put(event)
            else:
                path.unlink(missing_ok=True)

    async def _upload_loop(self) -> None:
        while True:
            event = await self.upload_queue.get()
            try:
                while True:
                    try:
                        await self._deliver(event)
                        event_path = Path(str(event.get("_event_path", "")))
                        event_path.unlink(missing_ok=True)
                        self.queued_delivery_paths.discard(event_path)
                        self.running.pop(str(event.get("attempt_id", "")), None)
                        break
                    except httpx.HTTPStatusError as exc:
                        if exc.response.status_code == 410:
                            self.log.warning("dynamic Seed delivery invalidated attempt=%s", event.get("attempt_id"))
                            event_path = Path(str(event.get("_event_path", "")))
                            event_path.replace(event_path.with_suffix(".invalidated"))
                            self.queued_delivery_paths.discard(event_path)
                            self.running.pop(str(event.get("attempt_id", "")), None)
                            break
                        self.log.warning("dynamic Seed delivery failed attempt=%s status=%s; retrying", event.get("attempt_id"), exc.response.status_code)
                    except (OSError, httpx.HTTPError) as exc:
                        self.log.warning("dynamic Seed delivery unavailable attempt=%s: %s; retrying", event.get("attempt_id"), exc)
                    await asyncio.sleep(5)
            finally:
                self.upload_queue.task_done()

    async def _deliver(self, event: dict[str, Any]) -> None:
        base = self.state.config["master_url"].rstrip("/")
        headers = {"Authorization": f"Bearer {self.state.node_token}"}
        attempt_id, token = event["attempt_id"], event["lease_token"]
        async with httpx.AsyncClient(timeout=120) as client:
            if event.get("state") == "completed":
                artifact = Path(str(event["artifact_path"]))
                if not artifact.is_file():
                    event["state"] = "failed"
                    event["error"] = "completed Seed artifact is missing"
                else:
                    artifact_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{attempt_id}:seed-result"))
                    with artifact.open("rb") as handle:
                        response = await client.put(f"{base}/api/v2/artifacts/{artifact_id}", data={"attempt_id": attempt_id, "lease_token": token}, files={"artifact": (artifact.name, handle, "application/octet-stream")}, headers=headers)
                        response.raise_for_status()
            response = await client.post(f"{base}/api/v2/seed-attempts/{attempt_id}/complete", json={"lease_token": token, "state": event.get("state"), "fe": event.get("fe", 0), "total_fe": event.get("total_fe", 0), "elapsed_seconds": event.get("elapsed_seconds", 0), "error": event.get("error", "")}, headers=headers)
            response.raise_for_status()
