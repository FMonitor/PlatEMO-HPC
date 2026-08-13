# Master / Worker v1 接口协议

当前版本只使用 `/api/v1` 调度协议，不保留旧的 Master 主动推送任务接口。所有标识符均为 UUID，时间使用 ISO-8601 UTC。

## 认证与模型

Master 启动时生成并持久化 `worker_join_token`。Worker 首次注册使用 Join Token，成功后获得仅属于该节点的 `node_token`；后续心跳、批次进度、产物和完成确认均使用 `Authorization: Bearer <node_token>`。

调度单位为 SeedRun：一个算法-问题实验点中的一个 seed。Master 在 Worker 心跳响应中把同一实验点的一组 SeedRun 分配为一个 BatchAttempt，并签发不可转发的 `lease_token`。任何写入都必须同时匹配 `batch_attempt_id + lease_token`，过期或已回收的租约返回 `410`。Worker 只报告容量，不能自行选择任务或 Seed。

## Worker 调用 Master

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/v1/workers/register` | 以 Join Token 注册并领取 Node Token |
| `POST` | `/api/v1/workers/{worker_id}/heartbeat` | 每 10 秒上报容量与运行批次；Master 响应中下发 assignment 和取消指令 |
| `POST` | `/api/v1/batch-attempts/{batch_attempt_id}/progress` | 上报批次和每个 Seed 的 FE 进度 |
| `POST` | `/api/v1/batch-attempts/{batch_attempt_id}/complete` | 确认 completed、failed 或 cancelled |
| `PUT` | `/api/v1/artifacts/{artifact_id}` | 上载结果 MAT 或日志产物 |

### 注册

```json
{
  "worker_id": "uuid",
  "name": "12600KF",
  "url": "http://10.0.0.12:6001",
  "capabilities": {"cpu_logical": 16, "gpu": [], "matlab_version": "R2024b"}
}
```

注册响应：

```json
{"worker_id":"uuid","node_token":"opaque-secret","heartbeat_seconds":10}
```

### 心跳分配

Worker 在空闲时以心跳报告容量；Master 在同一事务中分配 Seed 并返回 assignment。无任务时 `assignment` 为 `null`：

```json
{
  "available_batch_slots": 1,
  "max_concurrent_batches": 1,
  "configured_pool_workers": 10,
  "max_seeds_per_batch": 10,
  "running_batches": []
}
```

Worker 收到 assignment 后，必须在同一心跳响应处理周期内完成本地校验，并在下一次 API 写入中发送 `accepted` 或 `rejected`。`accepted` 可以是首个 `progress`，其中 `phase` 为 `accepted` 或 `running`；`rejected` 必须包含稳定错误码，例如 `insufficient_disk`、`profile_unavailable` 或 `input_incompatible`。Master 在签发后一个心跳周期内未收到 `accepted`，必须使该 BatchAttempt 失效并把全部未完成 SeedRun 恢复为 `pending`。`rejected` 不启动 MATLAB；Master 记录原因并立即回收该批次。

`running_batches[]` 用于续租和运行态核对。每项包含 `batch_attempt_id`、`experiment_point_id`、`lease_token`、`matlab_pid`、`configured_pool_workers`、`actual_pool_workers` 与可选 pool 摘要；Master 仅更新归属该 Worker 的有效批次。

```json
{
  "cancel_batch_attempt_ids": [],
  "assignment": {
    "batch_attempt_id": "uuid",
    "lease_token": "opaque-secret",
    "experiment_point_id": "uuid",
    "algorithm": {"name": "VRLFSEA", "parameters": {"sigma": 5}},
    "problem": {"name": "SMOP1", "parameters": {"M": 2, "D": 1000, "theta": 0.1}},
    "seeds": [1, 2, 3],
    "max_fe": 50000,
    "retain_points": 100,
    "cluster_profile": "local"
  }
}
```

### 进度与完成

```json
{
  "lease_token": "opaque-secret",
  "phase": "running",
  "completed_runs": 1,
  "failed_runs": 0,
  "running_runs": 2,
  "total_runs": 3,
  "pool": {"active": true, "workers": 8, "cluster_profile": "local"},
  "runs": [
    {"seed": 1, "state": "completed", "fe": 50000, "total_fe": 50000, "elapsed_seconds": 330, "error": ""},
    {"seed": 2, "state": "running", "fe": 21500, "total_fe": 50000, "elapsed_seconds": 140, "error": ""}
  ]
}
```

完成请求包含以上最终 `runs`、`state`、`exit_code` 和可选 `error`。Worker 应先上传 `result.mat`，再发送完成确认。若进度、产物或完成收到 `410`，Worker 必须立即终止该 BatchAttempt 的 MATLAB 进程树，停止一切后续进度、产物和完成写入，并保留本地文件；不能继续执行或覆盖新租约。

每次通过租约校验的 progress 都会刷新 `lease_deadline`，并持久化 PID、池状态和 Seed FE；长任务不能因首次确认后的固定截止时间被回收。

上传产物必须使用 `PUT /api/v1/artifacts/{artifact_id}`，以 multipart/form-data 传递 `experiment_point_id`、`batch_attempt_id`、`seed`、`lease_token`、`kind` 和 `artifact`；Master 存储 SHA-256 与大小。

## Master 提供给 UI

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/api/catalog` | 算法、问题、参数、Setting 文件和已有数据 |
| `PUT` | `/api/platemo-path` | 设置 PlatEMO 根目录 |
| `GET` | `/api/workers` | Worker 列表，不泄露 Token |
| `POST` | `/api/workers` | 手动录入已知节点 |
| `POST` | `/api/workers/probe-all` | 探测所有 Worker 的可达性 |
| `POST` | `/api/v1/experiments` | 创建正式实验点和全局 SeedRun 队列 |
| `GET` | `/api/v1/ui/tasks` | Seed 级状态、FE、耗时和 ETA 视图 |
| `POST` | `/api/v1/experiment-points/{experiment_point_id}/cancel` | 取消 pending Seed 并向运行批次下发取消请求 |
| `POST` | `/api/settings/load` | 导入 PlatEMO 或 Master 原生 MAT 设置 |
| `POST` | `/api/settings/save` | 导出 Master 原生 MAT 设置 |

实验创建请求携带算法和问题实例数组、运行次数、保留点数、可选最大 Worker 数以及选中的 Worker ID。可选 `cluster_profile` 写入实验快照；未指定时使用 Worker 能力声明的 profile。Master 仅向 profile 兼容的 Worker 分配 Seed 批次，不预先绑定 Worker。

## WatchDog 和失败语义

Worker 的标准心跳间隔为 10 秒。Master 仅将签名 heartbeat 视为续租；连续三次未收到，即超过 30 秒，Worker 标记 `suspect`，该 Worker 的 BatchAttempt 中未完成 SeedRun 回到 `pending`，原 BatchAttempt 标记 `reclaimed`。用户取消才将 SeedRun 置为 `cancelled`；已 `cancel_requested` 的 BatchAttempt 即使错误上报 `completed`，未终态 Seed 仍必须强制为 `cancelled`。进程失败、assignment 拒绝或租约失效均只回收未完成 SeedRun。旧 BatchAttempt 之后的写入必定得到 `410`。

| HTTP 状态 | Worker 行为 |
| --- | --- |
| `401` | 清除本地 Node Token，使用 Join Token 重新注册 |
| `404` | 记录错误，保留本地产物并人工处理 |
| `410` | 终止该 BatchAttempt 的 MATLAB 进程树，停止所有上报、产物和完成写入，保留本地文件 |
| `422` | 标记任务失败，不自动重试相同输入 |
| `429` / `503` | 指数退避重试通信，不能重复启动 MATLAB |
