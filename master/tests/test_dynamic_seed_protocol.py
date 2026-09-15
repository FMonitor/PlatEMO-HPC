"""Regression coverage for the V2 dynamic Seed Session protocol."""
from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from app import create_app


class DynamicSeedProtocolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.data_dir = Path(tempfile.mkdtemp())
        self.platemo_root = Path(tempfile.mkdtemp())
        self.app = create_app(self.data_dir, self.platemo_root)
        self.client = TestClient(self.app)
        self.worker_id = "11111111-1111-1111-1111-111111111111"
        registration = self.client.post(
            "/api/v2/workers/register",
            json={"worker_id": self.worker_id, "name": "test", "url": "http://worker",
                  "capabilities": {"cluster_profile": "local", "execution_mode": "dynamic_seed_session"}},
            headers={"Authorization": f"Bearer {self.app.state.worker_join_token}"},
        )
        self.assertEqual(registration.status_code, 200, registration.text)
        self.headers = {"Authorization": f"Bearer {registration.json()['node_token']}"}

    def create_experiment(self, algorithm: str = "A", runs: int = 4) -> None:
        response = self.client.post(
            "/api/v1/experiments",
            data={
                "algorithms_json": json.dumps([{"name": algorithm, "parameters": {}}]),
                "problems_json": json.dumps([{"name": "P", "parameters": {"maxFE": 100}}]),
                "runs": str(runs), "retain_points": "1",
            },
        )
        self.assertEqual(response.status_code, 200, response.text)

    def heartbeat(self, *, free_slots: int, running: list[dict[str, object]] | None = None,
                  session_id: str = "session-1", workers: int = 4) -> dict[str, object]:
        response = self.client.post(
            f"/api/v2/workers/{self.worker_id}/heartbeat",
            json={"session_id": session_id,
                  "pool": {"state": "ready", "configured_workers": workers, "actual_workers": workers},
                  "free_seed_slots": free_slots, "running_seeds": running or []},
            headers=self.headers,
        )
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def assignment(self) -> dict[str, object]:
        self.create_experiment(runs=1)
        return self.heartbeat(free_slots=1)["assignments"][0]  # type: ignore[index,return-value]

    def upload(self, assignment: dict[str, object]) -> None:
        response = self.client.put(
            f"/api/v2/artifacts/{assignment['attempt_id']}-artifact",
            data={"attempt_id": assignment["attempt_id"], "lease_token": assignment["lease_token"]},
            files={"artifact": ("seed.mat", b"seed-result", "application/octet-stream")},
            headers=self.headers,
        )
        self.assertEqual(response.status_code, 204, response.text)

    def test_v1_worker_protocol_is_retired(self) -> None:
        register = self.client.post("/api/v1/workers/register", json={})
        heartbeat = self.client.post(f"/api/v1/workers/{self.worker_id}/heartbeat", json={})
        self.assertEqual(register.status_code, 410)
        self.assertEqual(heartbeat.status_code, 410)
        self.assertEqual(register.json()["detail"], "protocol_v1_retired")

    def test_dynamic_session_fills_capacity_across_experiment_points(self) -> None:
        self.create_experiment("A", runs=30)
        self.create_experiment("B", runs=30)
        assignments = self.heartbeat(free_slots=40, workers=40)["assignments"]
        self.assertEqual(len(assignments), 40)
        self.assertEqual(len({item["attempt_id"] for item in assignments}), 40)
        self.assertEqual({item["algorithm"]["name"] for item in assignments}, {"A", "B"})

    def test_master_active_leases_bound_assignment_to_pool_capacity(self) -> None:
        self.create_experiment(runs=8)
        first = self.heartbeat(free_slots=4)["assignments"]
        self.assertEqual(len(first), 4)
        second = self.heartbeat(free_slots=4)["assignments"]
        self.assertEqual(second, [])

    def test_terminal_progress_requires_complete_with_artifact(self) -> None:
        assignment = self.assignment()
        rejected = self.client.post(
            f"/api/v2/seed-attempts/{assignment['attempt_id']}/progress",
            json={"lease_token": assignment["lease_token"], "state": "completed", "fe": 100, "total_fe": 100},
            headers=self.headers,
        )
        self.assertEqual(rejected.status_code, 422)
        complete = self.client.post(
            f"/api/v2/seed-attempts/{assignment['attempt_id']}/complete",
            json={"lease_token": assignment["lease_token"], "state": "completed", "fe": 100, "total_fe": 100},
            headers=self.headers,
        )
        self.assertEqual(complete.status_code, 409)

    def test_completed_seed_releases_slot_after_artifact_upload(self) -> None:
        self.create_experiment(runs=2)
        first = self.heartbeat(free_slots=1)["assignments"][0]
        self.upload(first)
        complete = self.client.post(
            f"/api/v2/seed-attempts/{first['attempt_id']}/complete",
            json={"lease_token": first["lease_token"], "state": "completed", "fe": 100, "total_fe": 100},
            headers=self.headers,
        )
        self.assertEqual(complete.status_code, 204, complete.text)
        replacement = self.heartbeat(free_slots=1)["assignments"]
        self.assertEqual(len(replacement), 1)

    def test_cancelled_point_is_returned_to_owning_worker(self) -> None:
        assignment = self.assignment()
        point_id = assignment["experiment_point_id"]
        cancelled = self.client.post(f"/api/v1/experiment-points/{point_id}/cancel")
        self.assertEqual(cancelled.status_code, 200, cancelled.text)
        response = self.heartbeat(free_slots=0, session_id="restarted")
        self.assertIn(assignment["attempt_id"], response["cancel_attempt_ids"])

    def test_single_seed_cancel_is_returned_to_worker(self) -> None:
        assignment = self.assignment()
        cancelled = self.client.post(f"/api/v2/seed-attempts/{assignment['attempt_id']}/cancel")
        self.assertEqual(cancelled.status_code, 204, cancelled.text)
        response = self.heartbeat(free_slots=0, session_id="restarted")
        self.assertIn(assignment["attempt_id"], response["cancel_attempt_ids"])

    def test_missing_seed_lease_is_reclaimed(self) -> None:
        assignment = self.assignment()
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            con.execute("UPDATE seed_attempts_v3 SET lease_deadline='2000-01-01T00:00:00+00:00' WHERE id=?", (assignment["attempt_id"],))
        self.app.state.reclaim_expired_dynamic_seeds()
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            state = con.execute("SELECT state FROM seed_runs_v2 WHERE batch_attempt_id IS NULL").fetchone()[0]
        self.assertEqual(state, "pending")

    def test_restart_reattaches_durable_delivery(self) -> None:
        assignment = self.assignment()
        response = self.heartbeat(
            free_slots=1,
            session_id="restarted",
            running=[{"attempt_id": assignment["attempt_id"], "lease_token": assignment["lease_token"],
                      "experiment_point_id": assignment["experiment_point_id"], "seed": assignment["seed"],
                      "fe": 100, "total_fe": 100, "elapsed_seconds": 2, "phase": "delivering"}],
        )
        self.assertEqual(response["assignments"], [])
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            session = con.execute("SELECT session_id,state FROM seed_attempts_v3 WHERE id=?", (assignment["attempt_id"],)).fetchone()
        self.assertEqual(session, ("restarted", "running"))

    def test_paused_worker_receives_no_assignment(self) -> None:
        self.create_experiment(runs=1)
        paused = self.client.post(f"/api/v1/workers/{self.worker_id}/dispatch-pause", json={"paused": True})
        self.assertEqual(paused.status_code, 200, paused.text)
        self.assertEqual(self.heartbeat(free_slots=1)["assignments"], [])

    def test_dynamic_artifact_is_materialized_by_algorithm(self) -> None:
        assignment = self.assignment()
        self.upload(assignment)
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            path = Path(con.execute("SELECT path FROM artifacts_v3 WHERE seed_attempt_id=?", (assignment["attempt_id"],)).fetchone()[0])
        self.assertEqual(path.name, "A_P_M0_D0_1.mat")
        self.assertEqual(path.parent.name, "A")
        self.assertEqual(path.read_bytes(), b"seed-result")


if __name__ == "__main__":
    unittest.main()
