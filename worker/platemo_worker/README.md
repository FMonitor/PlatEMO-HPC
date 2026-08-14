# Worker Runtime Modules

- `api.py`: FastAPI lifecycle, health endpoint, and CLI-facing service assembly.
- `runtime.py`: assignment lifecycle, Master heartbeat/progress client, MATLAB process ownership, recovery, and delivery orchestration.
- `settings.py`: safe local Settings MAT resolution and SHA-256 task snapshot fields.
- `summary.py`: progress-file reconstruction and durable batch summary helpers.

`../app.py` remains a small compatibility CLI entrypoint for `Start-Worker.ps1`.
