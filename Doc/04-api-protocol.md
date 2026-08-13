# API 与实时通信协议

版本前缀：`/api/v1`。所有时间使用 ISO-8601 UTC，所有标识符使用 UUID。Worker API 请求采用 `Authorization: Bearer <node-token>`；UI 采用用户会话或本地管理员 token。

## Master API

| 方法 | 路径 | 调用方 | 用途 |
| --- | --- | --- | --- |
| `POST` | `/workers/register` | Worker | 一次性注册并换取节点密钥 |
| `POST` | `/workers/{id}/heartbeat` | Worker | 心跳、能力、运行 Attempt 和进度 |
| `POST` | `/workers/{id}/lease` | Worker | 领取一个 Task 租约 |
| `POST` | `/tasks/{taskId}/attempts/{attemptId}/progress` | Worker | FE/ETA/日志 offset 上报 |
| `POST` | `/tasks/{taskId}/attempts/{attemptId}/complete` | Worker | 成功、失败、取消和产物元数据 |
| `PUT` | `/artifacts/{artifactId}` | Worker | 上传 MAT/日志，支持断点续传 |
| `GET` | `/catalog` | UI | 算法、问题、参数、Settings 列表 |
| `POST` | `/experiments` | UI | 创建草稿/快照 |
| `POST` | `/experiments/{id}/start` | UI | 根据快照生成 Task |
| `POST` | `/experiments/{id}/cancel` | UI | 停止未开始任务并取消运行任务 |
| `GET` | `/workers` | UI | Worker 状态、优先级和能力 |
| `PATCH` | `/workers/{id}` | UI | 名称、优先级、槽位、启停状态 |
| `POST` | `/settings/import` | UI | 上传并解析 MAT |
| `POST` | `/settings/export` | UI | 另存为实验配置 MAT |
| `GET` | `/events` | UI | 历史事件与审计 |

## Worker 领取任务

Worker 请求：

```json
{
  "free_slots": 2,
  "running_attempt_ids": ["..."],
  "platemo_commit": "b8687ee"
}
```

Master 返回 `204` 表示无任务，返回 `200` 表示授予租约：

```json
{
  "task_id": "uuid",
  "attempt_id": "uuid",
  "lease_token": "opaque-secret",
  "lease_expires_at": "2026-08-13T03:00:00Z",
  "algorithm": {"name": "VRLFSEA", "parameters": {"sigma": 5}},
  "problem": {"name": "SMOP1", "parameters": {"M": 2, "D": 100, "theta": 0.1}},
  "seed": 17,
  "max_fe": 50000,
  "retain_points": 100,
  "settings_artifact": {"url": "...", "sha256": "..."}
}
```

## Progress 上报

```json
{
  "lease_token": "opaque-secret",
  "state": "running",
  "pid": 38214,
  "fe": 21500,
  "total_fe": 50000,
  "elapsed_seconds": 312.8,
  "estimated_remaining_seconds": 413.2,
  "phase": "environmental-selection",
  "log_offset": 8119,
  "reported_at": "2026-08-13T02:47:00Z"
}
```

Master 拒绝过期租约、未知 Attempt 或 token 不匹配的进度。所有写操作以 `task_id + attempt_id + lease_token` 为幂等键。

## WebSocket：Master 到前端

路径：`/ws/v1/events`。事件至少包括：

```json
{"type":"worker.updated","worker_id":"uuid","status":"online","free_slots":2}
{"type":"task.progress","task_id":"uuid","fe":21500,"total_fe":50000,"eta_seconds":413}
{"type":"task.reclaimed","task_id":"uuid","old_worker_id":"uuid","reason":"three_missed_heartbeats"}
{"type":"task.completed","task_id":"uuid","artifact_id":"uuid"}
```

前端断线重连后以最后事件 ID 拉取遗漏事件；事件仅用于视图刷新，数据库状态才是权威来源。

## 错误与重试

| HTTP | 语义 | Worker 行为 |
| --- | --- | --- |
| 401/403 | 认证或租约无效 | 停止上报，重新注册或人工处理 |
| 409 | 幂等冲突或状态冲突 | 拉取 Task 状态，不重复执行 |
| 410 | 租约已过期 | 停止该 Attempt，保留本地产物 |
| 422 | 任务参数不合法 | 标失败，不自动重试 |
| 429/503 | Master 暂时不可用 | 指数退避，保持 MATLAB 不被重复启动 |

