# Master / Worker v1 接口协议

当前版本只使用 `/api/v1` 调度协议，不保留旧的 Master 主动推送任务接口。所有标识符均为 UUID，时间使用 ISO-8601 UTC。

## 认证与模型

Master 启动时生成并持久化 `worker_join_token`。Worker 首次注册使用 Join Token，成功后获得仅属于该节点的 `node_token`；后续心跳、租约、进度、产物和完成确认均使用 `Authorization: Bearer <node_token>`。

调度单位为 Task：一个算法、一个问题实例和多个 seed。每次领取产生一个 Attempt 与不可转发的 `lease_token`。任何写入都必须同时匹配 `task_id + attempt_id + lease_token`，过期或已回收的租约返回 `410`。

## Worker 调用 Master

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/v1/workers/register` | 以 Join Token 注册并领取 Node Token |
| `POST` | `/api/v1/workers/{worker_id}/heartbeat` | 每 10 秒上报能力、空闲槽位、运行 Attempt；返回取消指令 |
| `POST` | `/api/v1/workers/{worker_id}/lease` | 空闲 Worker 领取一个已分配 Task |
| `POST` | `/api/v1/tasks/{task_id}/attempts/{attempt_id}/progress` | 上报批次和每个 seed 的 FE 进度 |
| `POST` | `/api/v1/tasks/{task_id}/attempts/{attempt_id}/complete` | 确认 completed、failed 或 cancelled |
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

### 租约

`POST /lease` 请求体为 `{"free_slots": 1}`。无任务时响应 `{"task": null}`；有任务时返回：

```json
{
  "task": {
    "id": "uuid",
    "algorithm": {"name": "VRLFSEA", "parameters": {"sigma": 5}},
    "problem": {"name": "SMOP1", "parameters": {"M": 2, "D": 1000, "theta": 0.1}},
    "seeds": [1, 2, 3],
    "max_fe": 50000,
    "retain_points": 100,
    "cluster_profile": "local"
  },
  "attempt_id": "uuid",
  "attempt_no": 1,
  "lease_token": "opaque-secret"
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

完成请求包含以上最终 `runs`、`state`、`exit_code` 和可选 `error`。Worker 应先上传 `result.mat`，再发送完成确认。若进度或完成收到 `410`，Worker 停止该 Attempt 的后续上报并保留本地文件，不能继续执行或覆盖新租约。

上传产物以 multipart/form-data 传递 `task_id`、`attempt_id`、`lease_token`、`kind` 和 `artifact`；Master 存储 SHA-256 与大小。

## Master 提供给 UI

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/api/catalog` | 算法、问题、参数、Setting 文件和已有数据 |
| `PUT` | `/api/platemo-path` | 设置 PlatEMO 根目录 |
| `GET` | `/api/workers` | Worker 列表，不泄露 Token |
| `POST` | `/api/workers` | 手动录入已知节点 |
| `POST` | `/api/workers/probe-all` | 探测所有 Worker 的可达性 |
| `POST` | `/api/v1/experiments` | 创建正式实验 Task |
| `GET` | `/api/v1/ui/tasks` | Seed 级状态、FE、耗时和 ETA 视图 |
| `POST` | `/api/v1/tasks/{task_id}/cancel` | 取消排队任务或向运行 Worker 下发取消请求 |
| `POST` | `/api/settings/load` | 导入 PlatEMO 或 Master 原生 MAT 设置 |
| `POST` | `/api/settings/save` | 导出 Master 原生 MAT 设置 |

实验创建请求携带算法和问题实例数组、运行次数、保留点数、可选最大 Worker 数以及选中的 Worker ID。Master 按 `priority` 从高到低选择节点，再轮转分配每个算法-问题批次。

## WatchDog 和失败语义

Worker 的标准心跳间隔为 10 秒。Master 仅将签名 heartbeat 视为续租；连续三次未收到，即超过 30 秒，Worker 标记 `suspect`，处于 `leased` 或 `running` 的 Task 回到 `queued`，原 Attempt 标记 `reclaimed`。旧 Attempt 之后的写入必定得到 `410`。

| HTTP 状态 | Worker 行为 |
| --- | --- |
| `401` | 清除本地 Node Token，使用 Join Token 重新注册 |
| `404` | 记录错误，保留本地产物并人工处理 |
| `410` | 停止该 Attempt 上报和执行，保留本地文件 |
| `422` | 标记任务失败，不自动重试相同输入 |
| `429` / `503` | 指数退避重试通信，不能重复启动 MATLAB |
