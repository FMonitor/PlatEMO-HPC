"""Regression coverage for v2 BatchAttempt lease boundaries."""
from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from app import create_app


class LeaseBoundaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.data_dir = Path(tempfile.mkdtemp())
        self.app = create_app(self.data_dir, Path("H:/PlatEMO"))
        self.client = TestClient(self.app)
        self.worker_id = "11111111-1111-1111-1111-111111111111"
        registration = self.client.post(
            "/api/v1/workers/register",
            json={"worker_id": self.worker_id, "name": "test", "url": "http://worker",
                  "capabilities": {"cluster_profile": "local"}},
            headers={"Authorization": f"Bearer {self.app.state.worker_join_token}"},
        )
        self.headers = {"Authorization": f"Bearer {registration.json()['node_token']}"}

    def heartbeat(self, **overrides: object) -> dict[str, object]:
        payload: dict[str, object] = {
            "available_batch_slots": 1,
            "configured_pool_workers": 2,
            "actual_pool_workers": 0,
            "max_seeds_per_batch": 2,
            "running_batches": [],
        }
        payload.update(overrides)
        response = self.client.post(f"/api/v1/workers/{self.worker_id}/heartbeat", json=payload, headers=self.headers)
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def assignment(self) -> dict[str, object]:
        self.heartbeat()
        response = self.client.post(
            "/api/v1/experiments",
            data={
                "algorithms_json": json.dumps([{"name": "A", "parameters": {}}]),
                "problems_json": json.dumps([{"name": "P", "parameters": {"maxFE": 100}}]),
                "runs": "2", "retain_points": "1", "worker_ids": self.worker_id,
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        assignment = self.heartbeat()["assignment"]
        self.assertIsNotNone(assignment)
        return assignment  # type: ignore[return-value]

    def test_expired_lease_cannot_be_revived_by_progress(self) -> None:
        assignment = self.assignment()
        batch_id, token = assignment["batch_attempt_id"], assignment["lease_token"]
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            con.execute("UPDATE batch_attempts SET lease_deadline='2000-01-01T00:00:00+00:00' WHERE id=?", (batch_id,))
        response = self.client.post(f"/api/v1/batch-attempts/{batch_id}/progress", json={"lease_token": token, "phase": "running", "runs": []}, headers=self.headers)
        self.assertEqual(response.status_code, 410, response.text)
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            self.assertEqual(con.execute("SELECT state FROM batch_attempts WHERE id=?", (batch_id,)).fetchone()[0], "reclaimed")
            self.assertEqual(con.execute("SELECT COUNT(*) FROM seed_runs_v2 WHERE batch_attempt_id IS NULL AND state='pending'").fetchone()[0], 2)

    def test_heartbeat_requires_lease_token_to_renew_running_batch(self) -> None:
        assignment = self.assignment()
        batch_id, token = assignment["batch_attempt_id"], assignment["lease_token"]
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            before = con.execute("SELECT lease_deadline FROM batch_attempts WHERE id=?", (batch_id,)).fetchone()[0]
        self.heartbeat(available_batch_slots=0, running_batches=[{"batch_attempt_id": batch_id, "lease_token": "wrong", "matlab_pid": 77}])
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            after = con.execute("SELECT lease_deadline, pid FROM batch_attempts WHERE id=?", (batch_id,)).fetchone()
        self.assertEqual(after[0], before)
        self.assertIsNone(after[1])
        self.heartbeat(available_batch_slots=0, running_batches=[{"batch_attempt_id": batch_id, "lease_token": token, "matlab_pid": 77}])
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            renewed = con.execute("SELECT lease_deadline, pid FROM batch_attempts WHERE id=?", (batch_id,)).fetchone()
        self.assertGreaterEqual(renewed[0], before)
        self.assertEqual(renewed[1], 77)

    def test_zero_capacity_never_receives_assignment(self) -> None:
        response = self.client.post(
            "/api/v1/experiments",
            data={
                "algorithms_json": json.dumps([{"name": "A", "parameters": {}}]),
                "problems_json": json.dumps([{"name": "P", "parameters": {"maxFE": 100}}]),
                "runs": "1", "retain_points": "1", "worker_ids": self.worker_id,
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        response = self.heartbeat(configured_pool_workers=0, max_seeds_per_batch=0)
        self.assertIsNone(response["assignment"])


if __name__ == "__main__":
    unittest.main()
