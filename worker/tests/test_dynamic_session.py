"""Regression coverage for durable dynamic Seed delivery state."""
from __future__ import annotations

import json
import logging
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from platemo_worker.dynamic_session import DynamicSession


class DynamicSessionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        root = Path(self.temp.name)
        self.state = SimpleNamespace(
            root=root,
            data=root / "Data",
            config={"cluster_profile": "local", "master_url": "http://master", "matlab_exe": "matlab"},
            configured_pool_workers=4,
            node_token="node-token",
            log=logging.getLogger("dynamic-session-test"),
        )
        self.session = DynamicSession(self.state)

    def tearDown(self) -> None:
        self.temp.cleanup()

    async def test_terminal_outbox_event_reconstructs_delivery_after_restart(self) -> None:
        event = {
            "attempt_id": "attempt-1", "lease_token": "lease", "experiment_point_id": "point",
            "seed": 3, "state": "completed", "fe": 100, "total_fe": 100,
            "elapsed_seconds": 8, "artifact_path": str(self.session.runs / "attempt-1" / "attempt-1.mat"),
            "error": "",
        }
        path = self.session.outbox / "attempt-1-final.json"
        path.write_text(json.dumps(event), encoding="utf-8")

        await self.session._consume_outbox()

        self.assertEqual(self.session.running["attempt-1"]["phase"], "delivering")
        self.assertEqual(self.session._free_slots(), 0)  # No live pool yet.
        queued = self.session.upload_queue.get_nowait()
        self.assertEqual(queued["attempt_id"], "attempt-1")
        self.assertEqual(queued["phase"], "delivering")
        self.session.upload_queue.task_done()

    async def test_delivery_does_not_consume_a_computing_slot(self) -> None:
        self.session.running = {
            "running": {"phase": "running"},
            "delivering": {"phase": "delivering"},
        }
        self.session.process = SimpleNamespace(returncode=None)
        (self.session.data / "session.json").write_text(json.dumps({"actual_workers": 4}), encoding="utf-8")

        self.assertEqual(self.session._free_slots(), 3)

    async def test_cancellation_does_not_replace_completed_delivery(self) -> None:
        self.session.running = {"attempt-1": {"phase": "delivering"}}

        await self.session._cancel("attempt-1")

        self.assertFalse((self.session.data / "cancel" / "attempt-1.cancel").exists())

    async def test_matlab_start_failure_is_backed_off(self) -> None:
        with patch("platemo_worker.dynamic_session.asyncio.create_subprocess_exec",
                   new=AsyncMock(side_effect=OSError("matlab unavailable"))):
            with self.assertRaises(OSError):
                await self.session.start()

        self.assertGreater(self.session.next_start_at, 0)
        self.assertIsNone(self.session.process_log_handle)


if __name__ == "__main__":
    unittest.main()
