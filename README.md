# PlatEMO HPC

Personal, lightweight task dispatch for PlatEMO experiments. It does not use MATLAB Parallel Server: the Master dispatches work to independent Workers, and every Worker can run its own local MATLAB parallel pool.

## Layout

- `master/`: Web UI and task/result index. Listens on port `6080` (browser-safe).
- `worker/`: Copy this directory to the root of every PlatEMO checkout. Listens on port `6001`.

## Dynamic Seed Session Migration

The current release is being migrated from fixed `parfor` batches to a persistent MATLAB pool that accepts independent Seeds as slots become free. The protocol, recovery rules, storage behavior, and implementation sequence are defined in [Doc/08-dynamic-seed-session.md](Doc/08-dynamic-seed-session.md). Fixed BatchAttempt execution remains available only during the migration.

## Quick start

Start the Master from any directory. Its `Data/` directory is created next to the supplied data directory, or next to the current working directory by default.

```powershell
cd F:\Code\python\Platemo-HPC\master
.\Start-Master.ps1 -DataDir D:\PlatemoHpcData
```

Open `http://127.0.0.1:6080`, add Worker URLs such as `http://10.147.17.23:6001`, then create a task.

Master 的 Vue 前端构建产物已随仓库提供。修改前端后，在 `master/` 执行：

```powershell
.\Build-Frontend.ps1
```

它会把最新静态文件写入 `master/static/`，然后由 Master 同源托管。`Data/Setting*.mat` 可作为 PlatEMO 预设导入；界面导出的 `.mat` 是支持同问题多参数实例的 Master 原生格式，不可直接加载回 PlatEMO GUI。

On each compute node, copy `worker/` to `<PlatEMO_ROOT>\worker`, configure it, and run it:

```powershell
cd H:\PlatEMO\worker
.\Start-Worker.ps1
# Edit the generated config.json, then run Start-Worker.ps1 again.
```

The Worker keeps a durable local queue. With `auto_run` enabled, each batch runs in `Data/work/<task_id>/<attempt_no>/` with one MATLAB process, a cluster-profile `parpool`, per-seed results, a progress JSON, log, result MAT, and summary.

第一阶段只部署一个管理性质的 Worker 节点。一个 Worker 同时运行一个批次 MATLAB 进程；批次内的多个 `seeds` 由任务指定的 MATLAB cluster profile 和 `parpool` 分发。进度按 seed 上报，停止粒度是整个批次。完整约定见 [Doc/05-worker-deployment.md](Doc/05-worker-deployment.md)。

`worker/config.json` 的 `data_dir` 是 Worker 本机目录，用于保存待执行任务 JSON、运行状态和临时文件；它不是 Master 的结果目录。Master 的 `--data-dir` 才是集中保存 SQLite、上传的 settings.mat 和最终结果的目录。相对路径的 Worker `data_dir` 相对于 Worker 文件夹解析。
