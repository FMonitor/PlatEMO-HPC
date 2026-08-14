"""Regression coverage for v2 BatchAttempt lease boundaries."""
from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from io import BytesIO
from pathlib import Path

from fastapi.testclient import TestClient
import numpy as np
from scipy.io import savemat

from app import create_app
from platemo import native_settings_mat


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
                "runs": "2", "retain_points": "1",
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        assignment = self.heartbeat()["assignment"]
        self.assertIsNotNone(assignment)
        return assignment  # type: ignore[return-value]

    def dynamic_heartbeat(self, *, free_slots: int, running: list[dict[str, object]] | None = None,
                          session_id: str = "session-1") -> dict[str, object]:
        response = self.client.post(
            f"/api/v2/workers/{self.worker_id}/heartbeat",
            json={"session_id": session_id, "pool": {"state": "ready", "configured_workers": 4, "actual_workers": 4},
                  "free_seed_slots": free_slots, "running_seeds": running or []}, headers=self.headers,
        )
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

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

    def test_dynamic_session_fills_slots_across_experiment_points(self) -> None:
        for algorithm in ("A", "B"):
            response = self.client.post(
                "/api/v1/experiments",
                data={"algorithms_json": json.dumps([{"name": algorithm, "parameters": {}}]),
                      "problems_json": json.dumps([{"name": "P", "parameters": {"maxFE": 100}}]),
                      "runs": "30", "retain_points": "1"},
            )
            self.assertEqual(response.status_code, 200, response.text)
        response = self.client.post(
            f"/api/v2/workers/{self.worker_id}/heartbeat",
            json={"session_id": "session-1", "pool": {"state": "ready", "configured_workers": 40, "actual_workers": 40},
                  "free_seed_slots": 40, "running_seeds": []}, headers=self.headers,
        )
        self.assertEqual(response.status_code, 200, response.text)
        assignments = response.json()["assignments"]
        self.assertEqual(len(assignments), 40)
        self.assertEqual(len({item["attempt_id"] for item in assignments}), 40)
        self.assertEqual(len({(item["experiment_point_id"], item["seed"]) for item in assignments}), 40)
        self.assertEqual({item["algorithm"]["name"] for item in assignments}, {"A", "B"})

    def test_dynamic_seed_completes_then_immediately_refills_one_slot(self) -> None:
        response = self.client.post(
            "/api/v1/experiments",
            data={"algorithms_json": json.dumps([{"name": "A", "parameters": {}}]),
                  "problems_json": json.dumps([{"name": "P", "parameters": {"maxFE": 100}}]),
                  "runs": "5", "retain_points": "1"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        assignment = self.dynamic_heartbeat(free_slots=1)["assignments"][0]
        attempt, token = assignment["attempt_id"], assignment["lease_token"]
        task = self.client.get("/api/v1/ui/tasks").json()[0]
        self.assertEqual(task["worker_id"], self.worker_id)
        self.assertEqual(task["pool"]["actual_workers"], 4)
        progress = self.client.post(f"/api/v2/seed-attempts/{attempt}/progress", json={"lease_token": token, "state": "running", "fe": 70, "total_fe": 100, "elapsed_seconds": 3}, headers=self.headers)
        self.assertEqual(progress.status_code, 200, progress.text)
        uploaded = self.client.put(f"/api/v2/artifacts/{attempt}-artifact", data={"attempt_id": attempt, "lease_token": token}, files={"artifact": ("seed.mat", b"seed-result", "application/octet-stream")}, headers=self.headers)
        self.assertEqual(uploaded.status_code, 200, uploaded.text)
        complete = self.client.post(f"/api/v2/seed-attempts/{attempt}/complete", json={"lease_token": token, "state": "completed", "fe": 100, "total_fe": 100, "elapsed_seconds": 4}, headers=self.headers)
        self.assertEqual(complete.status_code, 200, complete.text)
        replacement = self.dynamic_heartbeat(free_slots=1)["assignments"]
        self.assertEqual(len(replacement), 1)
        self.assertNotEqual(replacement[0]["attempt_id"], attempt)

    def test_dynamic_progress_cannot_bypass_artifact_backed_completion(self) -> None:
        response = self.client.post(
            "/api/v1/experiments",
            data={"algorithms_json": json.dumps([{"name": "A", "parameters": {}}]),
                  "problems_json": json.dumps([{"name": "P", "parameters": {"maxFE": 100}}]),
                  "runs": "1", "retain_points": "1"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        assignment = self.dynamic_heartbeat(free_slots=1)["assignments"][0]
        rejected = self.client.post(
            f"/api/v2/seed-attempts/{assignment['attempt_id']}/progress",
            json={"lease_token": assignment["lease_token"], "state": "completed", "fe": 100, "total_fe": 100},
            headers=self.headers,
        )
        self.assertEqual(rejected.status_code, 422, rejected.text)
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            state = con.execute("SELECT state FROM seed_runs_v2 WHERE experiment_point_id=? AND seed=?", (assignment["experiment_point_id"], assignment["seed"])).fetchone()[0]
        self.assertEqual(state, "leased")

    def test_dynamic_point_cancel_is_returned_to_owning_session(self) -> None:
        response = self.client.post(
            "/api/v1/experiments",
            data={"algorithms_json": json.dumps([{"name": "A", "parameters": {}}]),
                  "problems_json": json.dumps([{"name": "P", "parameters": {"maxFE": 100}}]),
                  "runs": "1", "retain_points": "1"},
        )
        point_id = self.client.get("/api/v1/ui/tasks").json()[0]["task_id"]
        assignment = self.dynamic_heartbeat(free_slots=1)["assignments"][0]
        cancelled = self.client.post(f"/api/v1/experiment-points/{point_id}/cancel")
        self.assertEqual(cancelled.status_code, 200, cancelled.text)
        heartbeat = self.dynamic_heartbeat(free_slots=0, session_id="session-after-restart")
        self.assertIn(assignment["attempt_id"], heartbeat["cancel_attempt_ids"])

    def test_dynamic_single_seed_cancel_is_returned_to_worker(self) -> None:
        response = self.client.post(
            "/api/v1/experiments",
            data={"algorithms_json": json.dumps([{"name": "A", "parameters": {}}]),
                  "problems_json": json.dumps([{"name": "P", "parameters": {"maxFE": 100}}]),
                  "runs": "1", "retain_points": "1"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        assignment = self.dynamic_heartbeat(free_slots=1)["assignments"][0]
        cancelled = self.client.post(f"/api/v2/seed-attempts/{assignment['attempt_id']}/cancel")
        self.assertEqual(cancelled.status_code, 200, cancelled.text)
        heartbeat = self.dynamic_heartbeat(free_slots=0, session_id="session-after-restart")
        self.assertIn(assignment["attempt_id"], heartbeat["cancel_attempt_ids"])

    def test_dynamic_delivery_renews_lease_without_occupying_pool_slot(self) -> None:
        response = self.client.post(
            "/api/v1/experiments",
            data={"algorithms_json": json.dumps([{"name": "A", "parameters": {}}]),
                  "problems_json": json.dumps([{"name": "P", "parameters": {"maxFE": 100}}]),
                  "runs": "2", "retain_points": "1"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        first = self.dynamic_heartbeat(free_slots=1)["assignments"][0]
        before = sqlite3.connect(self.data_dir / "master.sqlite3").execute(
            "SELECT lease_deadline FROM seed_attempts_v3 WHERE id=?", (first["attempt_id"],)
        ).fetchone()[0]
        second = self.dynamic_heartbeat(free_slots=1, running=[{
            "attempt_id": first["attempt_id"], "lease_token": first["lease_token"],
            "experiment_point_id": first["experiment_point_id"], "seed": first["seed"],
            "fe": 100, "total_fe": 100, "elapsed_seconds": 2, "phase": "delivering",
        }])["assignments"]
        after = sqlite3.connect(self.data_dir / "master.sqlite3").execute(
            "SELECT lease_deadline,state FROM seed_attempts_v3 WHERE id=?", (first["attempt_id"],)
        ).fetchone()
        self.assertEqual(after[1], "running")
        self.assertGreater(after[0], before)
        self.assertEqual(len(second), 1)
        self.assertNotEqual(second[0]["attempt_id"], first["attempt_id"])

    def test_dynamic_expired_and_cancelled_leases_are_recovered_independently(self) -> None:
        response = self.client.post(
            "/api/v1/experiments",
            data={"algorithms_json": json.dumps([{"name": "A", "parameters": {}}]),
                  "problems_json": json.dumps([{"name": "P", "parameters": {"maxFE": 100}}]),
                  "runs": "2", "retain_points": "1"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        first = self.dynamic_heartbeat(free_slots=1)["assignments"][0]
        second = self.dynamic_heartbeat(free_slots=1)["assignments"][0]
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            con.execute("UPDATE seed_attempts_v3 SET lease_deadline='2000-01-01T00:00:00+00:00' WHERE id=?", (first["attempt_id"],))
        self.app.state.reclaim_expired_dynamic_seeds()
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            row = con.execute("SELECT state,batch_attempt_id FROM seed_runs_v2 WHERE experiment_point_id=? AND seed=?", (first["experiment_point_id"], first["seed"])).fetchone()
            self.assertEqual(row, ("pending", None))
        self.client.post(f"/api/v1/experiment-points/{second['experiment_point_id']}/cancel")
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            con.execute("UPDATE seed_attempts_v3 SET lease_deadline='2000-01-01T00:00:00+00:00' WHERE id=?", (second["attempt_id"],))
        self.app.state.reclaim_expired_dynamic_seeds()
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            row = con.execute("SELECT state FROM seed_runs_v2 WHERE experiment_point_id=? AND seed=?", (second["experiment_point_id"], second["seed"])).fetchone()
            self.assertEqual(row[0], "cancelled")

    def test_dynamic_worker_restart_reattaches_durable_delivery_to_new_session(self) -> None:
        response = self.client.post(
            "/api/v1/experiments",
            data={"algorithms_json": json.dumps([{"name": "A", "parameters": {}}]),
                  "problems_json": json.dumps([{"name": "P", "parameters": {"maxFE": 100}}]),
                  "runs": "1", "retain_points": "1"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        assignment = self.dynamic_heartbeat(free_slots=1)["assignments"][0]
        response = self.dynamic_heartbeat(free_slots=0, session_id="session-after-restart", running=[{
            "attempt_id": assignment["attempt_id"], "lease_token": assignment["lease_token"],
            "experiment_point_id": assignment["experiment_point_id"], "seed": assignment["seed"],
            "fe": 100, "total_fe": 100, "elapsed_seconds": 5, "phase": "delivering",
        }])
        self.assertEqual(response["assignments"], [])
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            attempt = con.execute("SELECT session_id,state,fe FROM seed_attempts_v3 WHERE id=?", (assignment["attempt_id"],)).fetchone()
        self.assertEqual(attempt, ("session-after-restart", "running", 100))

    def test_dynamic_rejected_artifact_upload_leaves_no_partial_file(self) -> None:
        response = self.client.post(
            "/api/v1/experiments",
            data={"algorithms_json": json.dumps([{"name": "A", "parameters": {}}]),
                  "problems_json": json.dumps([{"name": "P", "parameters": {"maxFE": 100}}]),
                  "runs": "1", "retain_points": "1"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        assignment = self.dynamic_heartbeat(free_slots=1)["assignments"][0]
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            con.execute("UPDATE seed_attempts_v3 SET lease_deadline='2000-01-01T00:00:00+00:00' WHERE id=?", (assignment["attempt_id"],))
        rejected = self.client.put(
            "/api/v2/artifacts/test-artifact",
            data={"attempt_id": assignment["attempt_id"], "lease_token": assignment["lease_token"]},
            files={"artifact": ("seed.mat", b"seed", "application/octet-stream")}, headers=self.headers,
        )
        self.assertEqual(rejected.status_code, 410, rejected.text)
        self.assertFalse(list(self.data_dir.rglob("*.part")))

    def test_paused_worker_receives_no_new_assignment(self) -> None:
        self.heartbeat()
        response = self.client.post(
            "/api/v1/experiments",
            data={
                "algorithms_json": json.dumps([{"name": "A", "parameters": {}}]),
                "problems_json": json.dumps([{"name": "P", "parameters": {"maxFE": 100}}]),
                "runs": "2", "retain_points": "1",
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        paused = self.client.post(f"/api/v1/workers/{self.worker_id}/dispatch-pause", json={"paused": True})
        self.assertEqual(paused.status_code, 200, paused.text)
        self.assertTrue(paused.json()["paused"])
        self.assertIsNone(self.heartbeat()["assignment"])
        resumed = self.client.post(f"/api/v1/workers/{self.worker_id}/dispatch-pause", json={"paused": False})
        self.assertEqual(resumed.status_code, 200, resumed.text)
        self.assertIsNotNone(self.heartbeat()["assignment"])

    def test_batch_delete_history_is_atomic(self) -> None:
        assignment = self.assignment()
        point_id = assignment["experiment_point_id"]
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            con.execute(
                "UPDATE seed_runs_v2 SET state='completed' WHERE experiment_point_id=?",
                (point_id,),
            )
        deleted = self.client.post(
            "/api/v1/seed-runs/history/delete",
            json={"runs": [
                {"experiment_point_id": point_id, "seed": 1},
                {"experiment_point_id": point_id, "seed": 2},
            ]},
        )
        self.assertEqual(deleted.status_code, 200, deleted.text)
        self.assertEqual(deleted.json()["count"], 2)
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            self.assertEqual(
                con.execute("SELECT COUNT(*) FROM seed_runs_v2 WHERE experiment_point_id=?", (point_id,)).fetchone()[0],
                0,
            )

    def test_batch_delete_history_rejects_mixed_states_without_deleting(self) -> None:
        assignment = self.assignment()
        point_id = assignment["experiment_point_id"]
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            con.execute(
                "UPDATE seed_runs_v2 SET state='completed' WHERE experiment_point_id=? AND seed=1",
                (point_id,),
            )
        rejected = self.client.post(
            "/api/v1/seed-runs/history/delete",
            json={"runs": [
                {"experiment_point_id": point_id, "seed": 1},
                {"experiment_point_id": point_id, "seed": 2},
            ]},
        )
        self.assertEqual(rejected.status_code, 409, rejected.text)
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            self.assertEqual(
                con.execute("SELECT state FROM seed_runs_v2 WHERE experiment_point_id=? AND seed=1", (point_id,)).fetchone()[0],
                "completed",
            )

    def test_unbound_experiment_is_shared_by_compatible_workers(self) -> None:
        second_worker_id = "22222222-2222-2222-2222-222222222222"
        registration = self.client.post(
            "/api/v1/workers/register",
            json={"worker_id": second_worker_id, "name": "second", "url": "http://second",
                  "capabilities": {"cluster_profile": "local"}},
            headers={"Authorization": f"Bearer {self.app.state.worker_join_token}"},
        )
        self.assertEqual(registration.status_code, 200, registration.text)
        second_headers = {"Authorization": f"Bearer {registration.json()['node_token']}"}
        response = self.client.post(
            "/api/v1/experiments",
            data={
                "algorithms_json": json.dumps([{"name": "A", "parameters": {}}]),
                "problems_json": json.dumps([{"name": "P", "parameters": {"maxFE": 100}}]),
                "runs": "4", "retain_points": "1",
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        first = self.heartbeat()["assignment"]
        self.assertIsNotNone(first)
        second = self.client.post(
            f"/api/v1/workers/{second_worker_id}/heartbeat",
            json={"available_batch_slots": 1, "configured_pool_workers": 2,
                  "actual_pool_workers": 0, "max_seeds_per_batch": 2, "running_batches": []},
            headers=second_headers,
        )
        self.assertEqual(second.status_code, 200, second.text)
        second_assignment = second.json()["assignment"]
        self.assertIsNotNone(second_assignment)
        self.assertEqual(set(first["seeds"]).intersection(second_assignment["seeds"]), set())

    def test_heartbeat_requires_lease_token_to_renew_running_batch(self) -> None:
        assignment = self.assignment()
        batch_id, token = assignment["batch_attempt_id"], assignment["lease_token"]
        accepted = self.client.post(
            f"/api/v1/batch-attempts/{batch_id}/progress",
            json={"lease_token": token, "phase": "running", "runs": []}, headers=self.headers,
        )
        self.assertEqual(accepted.status_code, 200, accepted.text)
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

    def test_assigned_batch_heartbeat_cannot_bypass_acceptance_deadline(self) -> None:
        assignment = self.assignment()
        batch_id, token = assignment["batch_attempt_id"], assignment["lease_token"]
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            before = con.execute("SELECT lease_deadline FROM batch_attempts WHERE id=?", (batch_id,)).fetchone()[0]
        self.heartbeat(available_batch_slots=0, running_batches=[{"batch_attempt_id": batch_id, "lease_token": token, "matlab_pid": 77}])
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            after = con.execute("SELECT lease_deadline, pid FROM batch_attempts WHERE id=?", (batch_id,)).fetchone()
        self.assertEqual(after[0], before)
        self.assertIsNone(after[1])

    def test_expired_running_batch_cannot_be_revived_by_heartbeat(self) -> None:
        assignment = self.assignment()
        batch_id, token = assignment["batch_attempt_id"], assignment["lease_token"]
        accepted = self.client.post(
            f"/api/v1/batch-attempts/{batch_id}/progress",
            json={"lease_token": token, "phase": "running", "runs": []}, headers=self.headers,
        )
        self.assertEqual(accepted.status_code, 200, accepted.text)
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            con.execute("UPDATE batch_attempts SET lease_deadline='2000-01-01T00:00:00+00:00' WHERE id=?", (batch_id,))
        response = self.heartbeat(available_batch_slots=0, running_batches=[{
            "batch_attempt_id": batch_id, "lease_token": token, "matlab_pid": 77,
        }])
        self.assertIn(batch_id, response["cancel_batch_attempt_ids"])
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            self.assertEqual(con.execute("SELECT state FROM batch_attempts WHERE id=?", (batch_id,)).fetchone()[0], "reclaimed")
            self.assertEqual(con.execute("SELECT COUNT(*) FROM seed_runs_v2 WHERE batch_attempt_id IS NULL AND state='pending'").fetchone()[0], 2)

    def test_heartbeat_persists_authorized_seed_summary(self) -> None:
        assignment = self.assignment()
        batch_id, token = assignment["batch_attempt_id"], assignment["lease_token"]
        accepted = self.client.post(
            f"/api/v1/batch-attempts/{batch_id}/progress",
            json={"lease_token": token, "phase": "running", "runs": []}, headers=self.headers,
        )
        self.assertEqual(accepted.status_code, 200, accepted.text)
        self.heartbeat(available_batch_slots=0, running_batches=[{
            "batch_attempt_id": batch_id, "lease_token": token, "matlab_pid": 77,
            "runs": [{"seed": 1, "state": "running", "fe": 25, "total_fe": 100,
                      "elapsed_seconds": 10, "error": ""}],
        }])
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            row = con.execute("SELECT state, fe, total_fe, elapsed_seconds FROM seed_runs_v2 WHERE batch_attempt_id=? AND seed=1", (batch_id,)).fetchone()
        self.assertEqual(row, ("running", 25, 100, 10.0))

    def test_completed_progress_without_artifact_is_requeued_on_reclaim(self) -> None:
        assignment = self.assignment()
        batch_id, token = assignment["batch_attempt_id"], assignment["lease_token"]
        progress = self.client.post(
            f"/api/v1/batch-attempts/{batch_id}/progress",
            json={"lease_token": token, "phase": "running", "runs": [
                {"seed": 1, "state": "completed", "fe": 100, "total_fe": 100},
                {"seed": 2, "state": "running", "fe": 50, "total_fe": 100},
            ]}, headers=self.headers,
        )
        self.assertEqual(progress.status_code, 200, progress.text)
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            con.execute("UPDATE batch_attempts SET lease_deadline='2000-01-01T00:00:00+00:00' WHERE id=?", (batch_id,))
        self.app.state.reclaim_expired_batches()
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            rows = con.execute(
                "SELECT seed, state, batch_attempt_id FROM seed_runs_v2 ORDER BY seed"
            ).fetchall()
        self.assertEqual(rows, [(1, "pending", None), (2, "pending", None)])

    def test_completed_heartbeat_without_artifact_is_requeued_on_reclaim(self) -> None:
        assignment = self.assignment()
        batch_id, token = assignment["batch_attempt_id"], assignment["lease_token"]
        accepted = self.client.post(
            f"/api/v1/batch-attempts/{batch_id}/progress",
            json={"lease_token": token, "phase": "running", "runs": []}, headers=self.headers,
        )
        self.assertEqual(accepted.status_code, 200, accepted.text)
        self.heartbeat(available_batch_slots=0, running_batches=[{
            "batch_attempt_id": batch_id, "lease_token": token,
            "runs": [{"seed": 1, "state": "completed", "fe": 100, "total_fe": 100}],
        }])
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            con.execute("UPDATE batch_attempts SET lease_deadline='2000-01-01T00:00:00+00:00' WHERE id=?", (batch_id,))
        self.app.state.reclaim_expired_batches()
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            row = con.execute(
                "SELECT state, batch_attempt_id FROM seed_runs_v2 WHERE seed=1"
            ).fetchone()
        self.assertEqual(row, ("pending", None))

    def test_running_batch_cannot_be_rejected(self) -> None:
        assignment = self.assignment()
        batch_id, token = assignment["batch_attempt_id"], assignment["lease_token"]
        accepted = self.client.post(
            f"/api/v1/batch-attempts/{batch_id}/progress",
            json={"lease_token": token, "phase": "running", "runs": []}, headers=self.headers,
        )
        self.assertEqual(accepted.status_code, 200, accepted.text)
        rejected = self.client.post(
            f"/api/v1/batch-attempts/{batch_id}/progress",
            json={"lease_token": token, "phase": "rejected", "runs": []}, headers=self.headers,
        )
        self.assertEqual(rejected.status_code, 409, rejected.text)
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            self.assertEqual(con.execute("SELECT state FROM batch_attempts WHERE id=?", (batch_id,)).fetchone()[0], "running")

    def test_complete_requires_valid_state_and_registered_artifact(self) -> None:
        assignment = self.assignment()
        batch_id, token = assignment["batch_attempt_id"], assignment["lease_token"]
        invalid = self.client.post(
            f"/api/v1/batch-attempts/{batch_id}/complete",
            json={"lease_token": token, "state": "unknown", "runs": []}, headers=self.headers,
        )
        self.assertEqual(invalid.status_code, 422, invalid.text)
        missing_artifact = self.client.post(
            f"/api/v1/batch-attempts/{batch_id}/complete",
            json={"lease_token": token, "state": "completed", "runs": [{"seed": 1, "state": "completed"}]}, headers=self.headers,
        )
        self.assertEqual(missing_artifact.status_code, 409, missing_artifact.text)

    def test_worker_failed_batch_releases_slot_when_partial_results_are_unavailable(self) -> None:
        """Worker contract: a MATLAB crash after one Seed must not deadlock delivery."""
        assignment = self.assignment()
        batch_id, token = assignment["batch_attempt_id"], assignment["lease_token"]
        complete = self.client.post(
            f"/api/v1/batch-attempts/{batch_id}/complete",
            json={"lease_token": token, "state": "failed", "error": "MATLAB exited 1", "runs": [
                {"seed": 1, "state": "completed", "fe": 100, "total_fe": 100},
                {"seed": 2, "state": "failed", "fe": 50, "total_fe": 100, "error": "MATLAB exited 1"},
            ]}, headers=self.headers,
        )
        self.assertEqual(complete.status_code, 200, complete.text)
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            batch_state = con.execute("SELECT state FROM batch_attempts WHERE id=?", (batch_id,)).fetchone()[0]
            runs = con.execute("SELECT seed, state, batch_attempt_id FROM seed_runs_v2 ORDER BY seed").fetchall()
        self.assertEqual(batch_state, "failed")
        self.assertEqual(runs, [(1, "pending", None), (2, "failed", batch_id)])
        reassignment = self.heartbeat(available_batch_slots=1)["assignment"]
        self.assertIsNotNone(reassignment)
        self.assertEqual(reassignment["seeds"], [1])

    def test_failed_batch_preserves_completed_seed_with_registered_artifact(self) -> None:
        assignment = self.assignment()
        batch_id, token, point_id = assignment["batch_attempt_id"], assignment["lease_token"], assignment["experiment_point_id"]
        uploaded = self.client.put(
            "/api/v1/artifacts/44444444-4444-4444-4444-444444444444",
            data={"experiment_point_id": point_id, "batch_attempt_id": batch_id, "lease_token": token,
                  "seed": "1", "kind": "seed_result"},
            files={"artifact": ("seed-1.mat", b"seed-1", "application/octet-stream")}, headers=self.headers,
        )
        self.assertEqual(uploaded.status_code, 200, uploaded.text)
        complete = self.client.post(
            f"/api/v1/batch-attempts/{batch_id}/complete",
            json={"lease_token": token, "state": "failed", "runs": [
                {"seed": 1, "state": "completed"}, {"seed": 2, "state": "failed"},
            ]}, headers=self.headers,
        )
        self.assertEqual(complete.status_code, 200, complete.text)
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            row = con.execute("SELECT state, batch_attempt_id FROM seed_runs_v2 WHERE seed=1").fetchone()
        self.assertEqual(row, ("completed", batch_id))

    def test_cancelled_batch_discards_completed_seed_without_artifact(self) -> None:
        assignment = self.assignment()
        batch_id, token = assignment["batch_attempt_id"], assignment["lease_token"]
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            con.execute("UPDATE batch_attempts SET state='cancel_requested' WHERE id=?", (batch_id,))
        complete = self.client.post(
            f"/api/v1/batch-attempts/{batch_id}/complete",
            json={"lease_token": token, "state": "cancelled", "runs": [
                {"seed": 1, "state": "completed", "fe": 100, "total_fe": 100},
                {"seed": 2, "state": "cancelled", "fe": 30, "total_fe": 100},
            ]}, headers=self.headers,
        )
        self.assertEqual(complete.status_code, 200, complete.text)
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            row = con.execute("SELECT state, batch_attempt_id FROM seed_runs_v2 WHERE seed=1").fetchone()
        self.assertEqual(row, ("cancelled", None))

    def test_per_seed_artifact_cannot_cover_other_completed_seeds(self) -> None:
        assignment = self.assignment()
        batch_id, token, point_id = assignment["batch_attempt_id"], assignment["lease_token"], assignment["experiment_point_id"]
        uploaded = self.client.put(
            "/api/v1/artifacts/33333333-3333-3333-3333-333333333333",
            data={"experiment_point_id": point_id, "batch_attempt_id": batch_id, "lease_token": token,
                  "seed": "1", "kind": "seed_result"},
            files={"artifact": ("seed-1.mat", b"seed-1", "application/octet-stream")}, headers=self.headers,
        )
        self.assertEqual(uploaded.status_code, 200, uploaded.text)
        completed = self.client.post(
            f"/api/v1/batch-attempts/{batch_id}/complete",
            json={"lease_token": token, "state": "completed", "runs": [
                {"seed": 1, "state": "completed"}, {"seed": 2, "state": "completed"},
            ]}, headers=self.headers,
        )
        self.assertEqual(completed.status_code, 409, completed.text)

    def test_worker_with_wrong_commit_is_not_assigned(self) -> None:
        response = self.client.post(
            "/api/v1/experiments",
            data={"algorithms_json": json.dumps([{"name": "A", "parameters": {}}]),
                  "problems_json": json.dumps([{"name": "P", "parameters": {"maxFE": 100}}]),
                  "runs": "1", "retain_points": "1", "required_platemo_commit": "expected"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertIsNone(self.heartbeat()["assignment"])

    def test_native_master_settings_are_never_distributed_to_worker(self) -> None:
        native = native_settings_mat({"algorithms": [{"name": "A", "parameters": {}}],
                                      "problems": [{"name": "P", "parameters": {}}], "execution": {}})
        response = self.client.post(
            "/api/v1/experiments",
            data={"algorithms_json": json.dumps([{"name": "A", "parameters": {}}]),
                  "problems_json": json.dumps([{"name": "P", "parameters": {"maxFE": 100}}]),
                  "runs": "1", "retain_points": "1"},
            files={"settings_upload": ("master-native.mat", native.read(), "application/octet-stream")},
        )
        self.assertEqual(response.status_code, 200, response.text)
        assignment = self.heartbeat()["assignment"]
        self.assertEqual(assignment["settings_file"], "")
        self.assertEqual(assignment["settings_download_url"], "")
        self.assertFalse(any(self.data_dir.joinpath("uploads").rglob("master-native.mat")))

    def test_standard_platemo_settings_are_verified_and_downloadable(self) -> None:
        setting = np.empty((1, 3), dtype=object)
        setting[0, 0] = np.array(["A"], dtype=object)
        setting[0, 1] = np.array(["P"], dtype=object)
        setting[0, 2] = np.array([], dtype=object)
        stream = BytesIO()
        savemat(stream, {"Setting": setting, "Environment": np.array([1, 1])})
        response = self.client.post(
            "/api/v1/experiments",
            data={"algorithms_json": json.dumps([{"name": "A", "parameters": {}}]),
                  "problems_json": json.dumps([{"name": "P", "parameters": {"maxFE": 100}}]),
                  "runs": "1", "retain_points": "1"},
            files={"settings_upload": ("Setting-test.mat", stream.getvalue(), "application/octet-stream")},
        )
        self.assertEqual(response.status_code, 200, response.text)
        assignment = self.heartbeat()["assignment"]
        self.assertEqual(assignment["settings_file"], "Setting-test.mat")
        download = self.client.get(assignment["settings_download_url"], headers=self.headers)
        self.assertEqual(download.status_code, 200, download.text)
        self.assertEqual(download.content, stream.getvalue())

    def test_zero_capacity_never_receives_assignment(self) -> None:
        response = self.client.post(
            "/api/v1/experiments",
            data={
                "algorithms_json": json.dumps([{"name": "A", "parameters": {}}]),
                "problems_json": json.dumps([{"name": "P", "parameters": {"maxFE": 100}}]),
                "runs": "1", "retain_points": "1",
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        response = self.heartbeat(configured_pool_workers=0, max_seeds_per_batch=0)
        self.assertIsNone(response["assignment"])

    def test_worker_cannot_receive_a_second_active_batch(self) -> None:
        self.assignment()
        second = self.heartbeat(available_batch_slots=1)
        self.assertIsNone(second["assignment"])

    def test_cancel_requested_timeout_cancels_unfinished_seeds(self) -> None:
        assignment = self.assignment()
        batch_id = assignment["batch_attempt_id"]
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            con.execute(
                "UPDATE batch_attempts SET state='cancel_requested', lease_deadline='2000-01-01T00:00:00+00:00' WHERE id=?",
                (batch_id,),
            )
        self.app.state.reclaim_expired_batches()
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            self.assertEqual(con.execute("SELECT state FROM batch_attempts WHERE id=?", (batch_id,)).fetchone()[0], "cancelled")
            self.assertEqual(con.execute("SELECT COUNT(*) FROM seed_runs_v2 WHERE batch_attempt_id IS NULL AND state='cancelled'").fetchone()[0], 2)

    def test_batch_cancel_is_delivered_by_next_heartbeat(self) -> None:
        assignment = self.assignment()
        batch_id = assignment["batch_attempt_id"]
        cancel = self.client.post(f"/api/v1/batch-attempts/{batch_id}/cancel")
        self.assertEqual(cancel.status_code, 200, cancel.text)
        heartbeat = self.heartbeat(available_batch_slots=0)
        self.assertIn(batch_id, heartbeat["cancel_batch_attempt_ids"])
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            self.assertEqual(con.execute("SELECT state FROM batch_attempts WHERE id=?", (batch_id,)).fetchone()[0], "cancel_requested")

    def test_artifact_id_cannot_change_logical_ownership(self) -> None:
        assignment = self.assignment()
        batch_id, token, point_id = assignment["batch_attempt_id"], assignment["lease_token"], assignment["experiment_point_id"]
        artifact_id = "22222222-2222-2222-2222-222222222222"
        fields = {"experiment_point_id": point_id, "batch_attempt_id": batch_id,
                  "lease_token": token, "kind": "batch_result"}
        first = self.client.put(
            f"/api/v1/artifacts/{artifact_id}", data=fields,
            files={"artifact": ("result.mat", b"first", "application/octet-stream")}, headers=self.headers,
        )
        self.assertEqual(first.status_code, 200, first.text)
        conflict = self.client.put(
            f"/api/v1/artifacts/{artifact_id}", data={**fields, "kind": "different_kind"},
            files={"artifact": ("result.mat", b"second", "application/octet-stream")}, headers=self.headers,
        )
        self.assertEqual(conflict.status_code, 409, conflict.text)
        changed_content = self.client.put(
            f"/api/v1/artifacts/{artifact_id}", data=fields,
            files={"artifact": ("result.mat", b"changed", "application/octet-stream")}, headers=self.headers,
        )
        self.assertEqual(changed_content.status_code, 409, changed_content.text)

    def test_seed_artifact_is_materialized_in_its_experiment_directory(self) -> None:
        assignment = self.assignment()
        batch_id, token, point_id = assignment["batch_attempt_id"], assignment["lease_token"], assignment["experiment_point_id"]
        artifact_id = "33333333-3333-3333-3333-333333333333"
        response = self.client.put(
            f"/api/v1/artifacts/{artifact_id}",
            data={"experiment_point_id": point_id, "batch_attempt_id": batch_id,
                  "lease_token": token, "seed": "1", "kind": "seed_result"},
            files={"artifact": ("seed-1.mat", b"seed-result", "application/octet-stream")}, headers=self.headers,
        )
        self.assertEqual(response.status_code, 200, response.text)
        with sqlite3.connect(self.data_dir / "master.sqlite3") as con:
            experiment_id = con.execute("SELECT experiment_id FROM experiment_points WHERE id=?", (point_id,)).fetchone()[0]
            path = Path(con.execute("SELECT path FROM artifacts_v2 WHERE id=?", (artifact_id,)).fetchone()[0])
        self.assertEqual(path, self.data_dir / "experiments" / experiment_id / "A" / "A_P_M0_D0_1.mat")
        self.assertEqual(path.read_bytes(), b"seed-result")


if __name__ == "__main__":
    unittest.main()
