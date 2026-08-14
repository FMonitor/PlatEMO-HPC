"""Regression coverage for recovered MATLAB process control."""
from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from app import WorkerState


class RecoveredProcessTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.root = Path(self.temp.name)
        self.config_path = self.root / "config.json"
        self.state = WorkerState({
            "platemo_root": str(self.root), "data_dir": "Data", "cluster_profile": "local",
            "auto_run": True, "matlab_exe": "matlab",
        }, self.config_path)
        self.batch_id = "11111111-1111-1111-1111-111111111111"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def recovered_record(self) -> tuple[Path, dict[str, object]]:
        work_dir = self.state.work / "point" / self.batch_id
        work_dir.mkdir(parents=True)
        task_json = work_dir / "task.json"
        task_json.write_text("{}", encoding="utf-8")
        expression = self.state._launch_expression(task_json, work_dir)
        record: dict[str, object] = {
            "batch_attempt_id": self.batch_id, "lease_token": "lease", "pid": 1234,
            "task_json": str(task_json), "command_sha256": self.state._command_fingerprint(expression),
        }
        self.state._process_record_path(work_dir).write_text("{}", encoding="utf-8")
        self.state.orphaned_pids[self.batch_id] = 1234
        return work_dir, record

    async def test_cancel_terminates_recovered_pid_and_keeps_work_directory(self) -> None:
        work_dir, _ = self.recovered_record()
        self.state.terminate_pid_tree = AsyncMock()  # type: ignore[method-assign]

        self.assertTrue(await self.state.cancel(self.batch_id))

        self.state.terminate_pid_tree.assert_awaited_once_with(1234)  # type: ignore[attr-defined]
        self.assertTrue(self.state._process_record_path(work_dir).is_file())

    async def test_lease_invalidation_terminates_recovered_pid_and_keeps_work_directory(self) -> None:
        work_dir, _ = self.recovered_record()
        self.state.terminate_pid_tree = AsyncMock()  # type: ignore[method-assign]

        await self.state.invalidate_batch(self.batch_id)

        self.state.terminate_pid_tree.assert_awaited_once_with(1234)  # type: ignore[attr-defined]
        self.assertTrue(self.state._process_record_path(work_dir).is_file())

    async def test_recovered_monitor_sends_progress_until_process_exits(self) -> None:
        work_dir, record = self.recovered_record()
        self.state.batches[self.batch_id] = {"lease_token": "lease", "matlab_pid": 1234}
        self.state._pid_matches_record = AsyncMock(side_effect=[True, False])  # type: ignore[method-assign]
        self.state.send_progress = AsyncMock(return_value=True)  # type: ignore[method-assign]

        await self.state.monitor_recovered_process(self.batch_id, work_dir, record)

        self.state.send_progress.assert_awaited_once_with(self.batch_id, "lease", work_dir)  # type: ignore[attr-defined]

    async def test_recovered_process_exit_persists_summary_and_delivers_result(self) -> None:
        work_dir, record = self.recovered_record()
        running = self.state.running / f"{self.batch_id}.json"
        payload = {"batch_attempt_id": self.batch_id, "experiment_point_id": "point", "lease_token": "lease",
                   "seeds": [1], "max_fe": 100}
        running.write_text(json.dumps(payload), encoding="utf-8")
        (work_dir / "result.mat").write_bytes(b"result")
        self.state.batches[self.batch_id] = {"lease_token": "lease", "matlab_pid": 1234}
        self.state._pid_matches_record = AsyncMock(return_value=False)  # type: ignore[method-assign]
        self.state.deliver_batch = AsyncMock(return_value=True)  # type: ignore[method-assign]

        await self.state.monitor_recovered_process(self.batch_id, work_dir, record)

        self.assertTrue((work_dir / "summary.json").is_file())
        self.assertFalse(running.exists())
        self.state.deliver_batch.assert_awaited_once()  # type: ignore[attr-defined]

    async def test_progress_includes_current_matlab_pid(self) -> None:
        work_dir, _ = self.recovered_record()
        (work_dir / "progress.json").write_text(json.dumps({
            "phase": "running", "runs": [{"seed": 1, "state": "running", "fe": 25,
                                                   "total_fe": 100, "elapsed_seconds": 10}],
            "pool": {"workers": 2},
        }), encoding="utf-8")
        self.state.config["master_url"] = "http://master"
        self.state.node_token = "node-token"
        self.state.batches[self.batch_id] = {"matlab_pid": 4321}
        captured: dict[str, object] = {}

        class Response:
            def raise_for_status(self) -> None:
                return None

        class Client:
            async def __aenter__(self) -> "Client":
                return self

            async def __aexit__(self, *_: object) -> None:
                return None

            async def post(self, _: str, json: dict[str, object], **__: object) -> Response:
                captured.update(json)
                return Response()

        with patch("platemo_worker.runtime.httpx.AsyncClient", return_value=Client()):
            self.assertTrue(await self.state.send_progress(self.batch_id, "lease", work_dir))
        self.assertEqual(captured["matlab_pid"], 4321)
        run = self.state.batches[self.batch_id]["runs"][0]
        self.assertEqual(run["fe"], 25)
        self.assertEqual(run["total_fe"], 100)
        self.assertEqual(run["eta_seconds"], 30.0)

    async def test_pid_validation_requires_matching_fingerprint_and_expression(self) -> None:
        work_dir, record = self.recovered_record()
        expression = self.state._launch_expression(work_dir / "task.json", work_dir)
        self.state._pid_command_line = AsyncMock(return_value=f'"matlab" -batch {expression}')  # type: ignore[method-assign]

        self.assertTrue(await self.state._pid_matches_record(record))
        record["command_sha256"] = "not-the-launch-fingerprint"
        self.assertFalse(await self.state._pid_matches_record(record))

    def test_launch_expression_adds_worker_wrapper_directory(self) -> None:
        work_dir, _ = self.recovered_record()
        expression = self.state._launch_expression(work_dir / "task.json", work_dir)
        wrapper_dir = Path(__file__).resolve().parents[1]

        self.assertTrue((wrapper_dir / "run_task.m").is_file())
        self.assertIn(f"addpath('{wrapper_dir}')", expression)


if __name__ == "__main__":
    unittest.main()
