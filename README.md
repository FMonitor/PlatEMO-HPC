# PlatEMO HPC

Personal, lightweight task dispatch for PlatEMO experiments. It does not use MATLAB Parallel Server: the Master dispatches work to independent Workers, and every Worker can run its own local MATLAB parallel pool.

## Layout

- `master/`: Web UI and task/result index. Listens on port `6000`.
- `worker/`: Copy this directory to the root of every PlatEMO checkout. Listens on port `6001`.

## Quick start

Start the Master from any directory. Its `Data/` directory is created next to the supplied data directory, or next to the current working directory by default.

```powershell
cd F:\Code\python\Platemo-HPC\master
.\Start-Master.ps1 -DataDir D:\PlatemoHpcData
```

Open `http://127.0.0.1:6000`, add Worker URLs such as `http://10.147.17.23:6001`, then create a task.

On each compute node, copy `worker/` to `<PlatEMO_ROOT>\worker`, configure it, and run it:

```powershell
cd H:\PlatEMO\worker
.\Start-Worker.ps1
# Edit the generated config.json, then run Start-Worker.ps1 again.
```

The Worker currently queues submitted tasks and accepts result uploads. Integrate `matlab_runner.py` with the local PlatEMO release before enabling automatic execution.

`worker/config.json` 的 `data_dir` 是 Worker 本机目录，用于保存待执行任务 JSON、运行状态和临时文件；它不是 Master 的结果目录。Master 的 `--data-dir` 才是集中保存 SQLite、上传的 settings.mat 和最终结果的目录。相对路径的 Worker `data_dir` 相对于 Worker 文件夹解析。
