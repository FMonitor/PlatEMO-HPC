# Master / Worker v2 接口协议

动态 Seed Session 是唯一 Worker 协议：使用 `/api/v2/workers/register` 和 `/api/v2/workers/{worker_id}/heartbeat` 逐个签发 `SeedAttempt`，允许不同实验点共享同一 MATLAB parpool 的空闲槽位。Worker 使用的 `/api/v1` 注册、心跳、BatchAttempt 和产物接口已退役并直接返回 `410 protocol_v1_retired`。

当前版本只使用 `/api/v1` 调度协议，不保留旧的 Master 主动推送任务接口。所有标识符均为 UUID，时间使用 ISO-8601 UTC。

## 认证与模型

Master 启动时生成并持久化 `worker_join_token`。Worker 首次注册使用 Join Token，成功后获得仅属于该节点的 `node_token`；后续心跳、批次进度、产物和完成确认均使用 `Authorization: Bearer <node_token>`。

调度单位为 SeedRun：一个算法-问题实验点中的一个 seed。Master 在 Worker 心跳响应中把同一实验点的一组 SeedRun 分配为一个 BatchAttempt，并签发不可转发的 `lease_token`。任何写入都必须同时匹配 `batch_attempt_id + lease_token`，过期或已回收的租约返回 `410`。Worker 只报告容量，不能自行选择任务或 Seed。

## Worker 调用 Master

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/v2/workers/register` | 以 Join Token 注册并领取 Node Token |
| `POST` | `/api/v2/workers/{worker_id}/heartbeat` | 每 2 秒上报并行池容量与运行 Seed；Master 响应中下发 assignment 和取消指令 |
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

Worker 收到 assignment 后，必须在同一心跳响应处理周期内完成本地校验，并在下一次 API 写入中发送 `accepted` 或 `rejected`。`cluster_profile` 必须等于 Worker 已探测的本机 profile；不一致时以 `profile_unavailable` 拒绝，不得启动 MATLAB。`accepted` 可以是首个 `progress`，其中 `phase` 为 `accepted` 或 `running`；`rejected` 必须包含稳定错误码，例如 `insufficient_disk`、`profile_unavailable` 或 `input_incompatible`。Master 在签发后一个心跳周期内未收到 `accepted`，必须使该 BatchAttempt 失效并把全部未完成 SeedRun 恢复为 `pending`。`rejected` 不启动 MATLAB；Master 记录原因并立即回收该批次。

`running_batches[]` 用于续租和运行态核对。每项包含 `batch_attempt_id`、`experiment_point_id`、`lease_token`、`matlab_pid`、`configured_pool_workers`、`actual_pool_workers`、逐 Seed FE/耗时摘要与可选 pool 摘要；Master 仅在同一写事务中确认归属、token、`accepted/running` 状态及 `lease_deadline > now()` 后，才更新 PID、池状态、Seed 摘要和租约。已过期批次会在该事务中回收，并在 heartbeat 响应的 `cancel_batch_attempt_ids` 中返回，不能被旧 Worker 复活。`assigned` 批次不能通过心跳续租，必须在确认窗口内以 progress 报告 `accepted`、`running` 或 `rejected`。

Worker 重启恢复时，只有同时匹配 `running/<batch_attempt_id>.json` 的 batch ID、lease token、`task.json` 路径，以及 `process.json` 中与 MATLAB `-batch` 启动表达式对应的 SHA-256 命令指纹，才可继续上报该批次。恢复期间 Worker 继续读取 `progress.json` 并发送 progress；取消指令或 `410` 与普通批次相同，必须终止恢复 MATLAB 的进程树并停止该租约的后续写入。

Master 只在 `available_batch_slots >= 1`、`configured_pool_workers >= 1` 且 `max_seeds_per_batch >= 1` 时签发 assignment，批次大小不超过后两者的较小值。`running_batches[]` 必须携带与 BatchAttempt 一致的 `lease_token`；缺失或不匹配时 Master 不续租也不更新 PID/池状态。Worker 自报容量只是必要条件；Master 在签发事务内还必须确认该 Worker 没有 `assigned`、`accepted`、`running` 或 `cancel_requested` 的 BatchAttempt，因此单个 Worker 同时最多持有一个有效批次。

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
    "M": 2,
    "D": 1000,
    "problem_parameter_values": [{"value": 0.1}],
    "retain_points": 20,
    "cluster_profile": "local"
  }
}
```

`N`、`M`、`D`、`max_fe` 与 `progress_interval_fe` 是顶层通用环境参数，Worker 必须显式传给 PlatEMO；后者规定同一 Seed 两次非终态 FE 上报的最小间隔。`algorithm_parameter_values` 与 `problem_parameter_values` 都是按 Master 目录参数定义顺序排列的数组；前者仅对应算法 `ParameterSet` 位置参数，后者仅对应问题的自定义 `ParameterSet` 位置参数。每项使用 `{ "value": ... }` 保留位置，即使采用默认值也不得省略。Worker 不得按任一 JSON object 的字段顺序转换为位置参数。

`settings_file` 是可选的 MATLAB Settings 基线，不是另一个可覆盖 assignment 的参数源。Worker 将它解析为本机允许路径、计算 SHA-256、写入 `task.json`，并由 `run_task.m` 实际加载后验证其包含已签发算法和问题；路径、摘要与 `applied=true` 被写入每个 Seed MAT 的 `task_snapshot`。若缺失、摘要不符或不包含任务实例，Worker 必须以 `input_incompatible` 拒绝或使该批次失败。

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

完成请求的 `state` 只能是 `completed`、`failed` 或 `cancelled`。`rejected` 只允许由 `assigned` 批次报告，运行中的批次不得回退。`state=completed` 时，若请求包含任一 `completed` Seed，Worker 必须先登记该 Seed 的可恢复 `seed_result` 产物，否则 Master 返回 `409`。`state=failed` 时，Master 接受完成确认并保留已由产物覆盖的 completed Seed；尚无产物的 completed Seed 原子恢复为 `pending`，使 Worker 能释放槽位而结果不会丢失。若批次由 Master 取消，Worker 必须把所有非 `completed`、`failed`、`cancelled` 的 runs 显式改为 `cancelled` 后再完成确认；任何未由产物覆盖的 completed Seed 也必须由 Master 改为 `cancelled` 并解除批次绑定。Worker 对每个成功 Seed 先上传 `runs/seed-<seed>.mat`，再发送完成确认；不生成聚合 `result.mat`。若进度、产物或完成收到 `410`，Worker 必须立即终止该 BatchAttempt 的 MATLAB 进程树，停止一切后续进度、产物和完成写入，并保留本地文件；不能继续执行或覆盖新租约。

每次通过租约校验的 progress 都会刷新 `lease_deadline`，并持久化 PID、池状态和 Seed FE；长任务不能因首次确认后的固定截止时间被回收。`progress` 或 heartbeat 中的 `completed` 仅表示 Worker 已计算该 Seed，不是可恢复终态；租约回收时，只有已由 `seed_result` 等单 Seed 产物覆盖，或已由 `seed=NULL, kind=batch_result` 聚合产物覆盖的 completed Seed 会保留。其余 completed Seed 与非终态 Seed 一样恢复为 `pending`（用户取消时为 `cancelled`），以避免丢失尚未上传的结果。

`progress`、`artifact` 和 `complete` 必须各自在同一个 `BEGIN IMMEDIATE` 写事务内校验 `lease_deadline > now()`，并完成对应的批次、Seed 或 Artifact 写入。截止时间已到达时 Master 在该事务内立即回收未完成 Seed 并返回 `410`，不能等待 WatchDog 的下一次轮询，也不能被旧 Worker 的进度重新续租。

上传产物必须使用 `PUT /api/v1/artifacts/{artifact_id}`，以 multipart/form-data 传递 `experiment_point_id`、`batch_attempt_id`、`seed`、`lease_token`、`kind` 和 `artifact`；Master 存储 SHA-256 与大小。同一逻辑产物的网络重试必须复用相同 `artifact_id`，Master 对该 PUT 必须幂等；若同一 ID 已属于不同的实验点、批次、Seed 或 kind，或摘要/大小不同，返回 `409`，不得覆盖审计记录。完成确认中存在 completed Seed 时，必须存在 `seed=NULL, kind=batch_result` 的批次聚合产物，或每个 completed Seed 各有至少一个产物；单个 Seed 的产物不能覆盖整个批次。Worker 在 artifact 成功后才可发送 complete；网络失败时保存交付清单、继续续租并重试，不能释放批次槽位或重新计算。

## Master 提供给 UI

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/api/catalog` | 算法、问题、参数、Setting 文件和已有数据 |
| `PUT` | `/api/platemo-path` | 设置 PlatEMO 根目录 |
| `GET` | `/api/workers` | Worker 列表，不泄露 Token |
| `POST` | `/api/v1/seed-runs/history/delete` | 原子删除所选已完成或失败的 Seed 任务记录；请求体为 `runs:[{experiment_point_id,seed}]`，不删除 MAT 产物 |
| `POST` | `/api/workers` | 手动录入已知节点 |
| `POST` | `/api/workers/probe-all` | 探测所有 Worker 的可达性 |
| `POST` | `/api/v1/experiments` | 创建正式实验点和全局 SeedRun 队列 |
| `GET` | `/api/v1/ui/tasks` | Seed 级状态、FE、耗时和 ETA 视图 |
| `POST` | `/api/v1/experiment-points/{experiment_point_id}/cancel` | 取消 pending Seed 并向运行批次下发取消请求 |
| `POST` | `/api/v1/batch-attempts/{batch_attempt_id}/cancel` | 请求取消一个 Worker 当前持有的 Seed 批次 |
| `POST` | `/api/settings/load` | 导入 PlatEMO 或 Master 原生 MAT 设置 |
| `POST` | `/api/settings/save` | 导出 Master 原生 MAT 设置 |

实验创建请求携带算法和问题实例数组、运行次数与保留点数，不携带 Worker ID 或每实验 Worker 上限。新实验对所有环境兼容且未暂停接单的 Worker 可见；暂停按钮是人工控制节点后续接单的唯一方式。可选 `cluster_profile` 写入实验快照；未指定时使用 Worker 能力声明的 profile。可选 `settings_file`、`required_platemo_commit` 和 `minimum_disk_free_bytes` 会随 assignment 下发，Worker 在 accepted 前校验。UI 上传 MAT 时，Master 先解析格式：`platemo-hpc-settings` 仅用于 UI 配置恢复，绝不保存或下发给 Worker；仅标准 `platemo-setting` 且覆盖本次全部算法与问题的文件才保存，并在 assignment 中下发 `settings_file`、`settings_sha256` 和受 Node Token 保护的 `settings_download_url`。Worker 下载到本机 `PlatEMO/Data`、校验 SHA-256 后才可 accepted。Master 会先按 Worker capabilities 的 profile、commit 和可用磁盘筛选；Worker 返回确定性拒绝时，Master 记录 Worker-实验点不兼容，避免同一 Seed 无限重新分配。

## WatchDog 和失败语义

Worker 的标准心跳间隔为 10 秒。Master 仅将签名 heartbeat 视为续租；连续三次未收到，即超过 30 秒，Worker 标记 `suspect`，该 Worker 的 BatchAttempt 中未完成 SeedRun 回到 `pending`，原 BatchAttempt 标记 `reclaimed`。WatchDog 同样扫描 `cancel_requested`：取消确认超时或节点失联时，将该批次中全部未终态 SeedRun 置为 `cancelled`，批次标记 `cancelled`，绝不重新入队。用户取消才将 SeedRun 置为 `cancelled`；已 `cancel_requested` 的 BatchAttempt 即使错误上报 `completed`，未终态 Seed 仍必须强制为 `cancelled`。进程失败、assignment 拒绝或租约失效均只回收未完成 SeedRun。旧 BatchAttempt 之后的写入必定得到 `410`。

| HTTP 状态 | Worker 行为 |
| --- | --- |
| `401` | 清除本地 Node Token，使用 Join Token 重新注册 |
| `404` | 记录错误，保留本地产物并人工处理 |
| `410` | 终止该 BatchAttempt 的 MATLAB 进程树，停止所有上报、产物和完成写入，保留本地文件 |
| `422` | 标记任务失败，不自动重试相同输入 |
| `429` / `503` | 指数退避重试通信，不能重复启动 MATLAB |
