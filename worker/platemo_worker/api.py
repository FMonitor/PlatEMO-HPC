"""FastAPI assembly for the Worker control and health surface."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException

from .runtime import WorkerState, now
from .dynamic_session import DynamicSession


def create_app(config_path: Path) -> FastAPI:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    state = WorkerState(config, config_path)
    dynamic = DynamicSession(state) if config.get("execution_mode") == "dynamic_seed_session" else None
    state.save_config()
    app = FastAPI(title="PlatEMO HPC Worker")
    app.state.worker = state

    def verify(token: str | None, authorization: str | None = None) -> None:
        expected = str(config.get("node_token", ""))
        supplied = authorization[7:] if authorization and authorization.lower().startswith("bearer ") else token
        if expected and supplied != expected:
            raise HTTPException(401, "Invalid worker token")

    @app.on_event("startup")
    async def start_runtime() -> None:
        state.schedule_pool_capacity_probe()
        if dynamic is not None:
            await dynamic.recover_previous_supervisor()
            app.state.control_task = asyncio.create_task(dynamic.run())
            app.state.dynamic_session = dynamic
            return
        await state.recover_running_processes()
        await state.resume_deliveries()
        app.state.control_task = asyncio.create_task(state.control_loop())

    @app.on_event("shutdown")
    async def stop_runtime() -> None:
        task = getattr(app.state, "control_task", None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        if dynamic is not None:
            await dynamic.stop()
        await state.stop_pool_capacity_probe()

    @app.post("/api/v1/health")
    async def health(payload: dict[str, Any], x_worker_token: str | None = Header(None),
                     authorization: str | None = Header(None)) -> dict[str, Any]:
        verify(x_worker_token, authorization)
        if dynamic is not None:
            return {"status": "ready" if dynamic.actual_workers else "starting", "worker_id": state.worker_id,
                    "queued": len(dynamic.inbox.glob("*.json")), "running": len(dynamic.running),
                    "available_batch_slots": dynamic._free_slots(),
                    "configured_pool_workers": state.configured_pool_workers,
                    "actual_pool_workers": dynamic.actual_workers,
                    "max_seeds_per_batch": dynamic.actual_workers,
                    "capabilities": state.capabilities(), "time": now()}
        return {"status": "ready", "worker_id": state.worker_id, "queued": len(state.queued()),
                "running": len(state.processes), "available_batch_slots": int(state.can_accept_assignment()),
                "configured_pool_workers": state.configured_pool_workers,
                "max_seeds_per_batch": state.max_seeds_per_batch(),
                "capabilities": state.capabilities(), "time": now()}

    return app
