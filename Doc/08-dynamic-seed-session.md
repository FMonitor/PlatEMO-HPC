# 动态 Seed 会话调度

## 目标

一个 Worker 只维护一个常驻无头 MATLAB 进程及其一个并行池。Master 不再把一个实验点的固定 Seed 列表交给 `parfor` 执行；而是按 Worker 当前空闲的池槽位持续下发独立 Seed。任一 Seed 完成后立即保存并上传 MAT，随后由 Master 以兼容的待运行 Seed 填补该槽位。

这使 `30` 次运行的实验点可以与其它实验点共同填满 `40` 个池槽位。一个 Worker 的“暂停接单”只阻止新 Seed 分配，不停止已提交的 Seed。

## 运行实体

| 实体 | 所有者 | 含义 |
| --- | --- | --- |
| `WorkerSession` | Worker | 一个存活的 MATLAB Supervisor 和一个 `parpool`；每 Worker 最多一个 |
| `SeedAttempt` | Master | 一个 `(experiment_point_id, seed)` 的独立租约、状态和结果归属 |
| `SeedAssignment` | Master -> Worker | 含 Seed 运行所需的完整快照；不含其它 Seed 的共享可变状态 |
| `SeedArtifact` | Worker -> Master | 一个完成 Seed 的 MAT，使用固定 `artifact_id` 幂等上传 |

旧 `BatchAttempt` 仅保留给已有记录的读取、交付和 WatchDog 回收。动态调度路径不得创建新的 BatchAttempt。

## Master 协议

用户可通过 `POST /api/v2/seed-attempts/{attempt_id}/cancel` 取消单个动态 Seed。Worker 下一次 Session 心跳收到 `cancel_attempt_ids` 后，只向对应 MATLAB Future 写入取消命令，不影响其它 Future；取消的 Seed 不会重新入队。

Worker 每 2 秒发送 `POST /api/v2/workers/{worker_id}/heartbeat`：

```json
{
  "session_id": "uuid",
  "pool": {"state": "ready", "configured_workers": 40, "actual_workers": 40},
  "free_seed_slots": 6,
  "running_seeds": [
    {"attempt_id": "uuid", "experiment_point_id": "uuid", "seed": 8, "fe": 12000, "total_fe": 50000, "elapsed_seconds": 21}
  ]
}
```

Master 在同一短 SQLite 事务内续租 `running_seeds`，并最多返回 `free_seed_slots` 个 `assignments`。每个 assignment 是单个 Seed 的完整参数快照，包含独立 `attempt_id`、`lease_token`、`experiment_point_id`、算法、问题、N/M/D/maxFE、自定义参数、Settings 摘要和环境约束。

选择规则：只选择 Worker 兼容、未暂停、未取消的 pending Seed；允许不同算法和问题混合，但每个 Seed 单独校验 Profile、PlatEMO commit、磁盘下限和 Settings。Master 不按实验点的运行次数限制一次会话的装填量。

`POST /api/v2/seed-attempts/{attempt_id}/progress` 和 `PUT /api/v2/artifacts/{artifact_id}` 均以 `(attempt_id, lease_token)` 验证。`completed` 状态只有在对应 `seed_result` 已登记后才是可恢复终态。租约失效返回 `410`，Worker 必须停止该 Seed 的后续写入；其余 Seed 保持运行。

## Worker 与 MATLAB

Worker 启动后先探测 Profile，再启动 MATLAB Supervisor。Supervisor 创建一次 `parpool(profile)`，使用 `parfeval` 为每个 assignment 提交一个独立运行函数。它从 Worker 本地 inbox 读取新 assignment，完成时向 outbox 写入：

- `started`、FE、耗时和当前实际池大小；
- `completed` / `failed` / `cancelled`；
- `runs/<attempt_id>.mat` 的稳定文件路径。

Worker 负责把 outbox 的进度上报给 Master。确认 MAT 文件已关闭后立即上传，使用单一上传队列限制并发（默认 `1`），以错峰磁盘和网络 I/O。MATLAB future 结束即释放计算槽位，Worker 可立即领取替补 Seed；原 Seed 转为 `delivering`，继续在心跳中续租，直至 artifact 与 complete 都被 Master 确认。交付重试不占用 MATLAB 槽位。

MATLAB Supervisor 或 Worker 重启时：先验证 PID 与 session 记录；不能验证则终止遗留进程树，并由 Master 回收未续租计算 Seed。Worker 必须从 durable outbox 恢复 `delivering` Seed，并以相同 `(worker_id, lease_token)` 让 Master 将该 Seed 重新绑定到新 `session_id`，持续续租直到上传完成。已上传的 MAT 不重算。

## Master 存储与性能

产物内容写入临时文件、校验 SHA-256 后原子替换到 `Data/experiments/<experiment_id>/<algorithm>/`。磁盘 I/O 与哈希不在 SQLite `BEGIN IMMEDIATE` 事务内；事务只做租约校验、幂等冲突检查和元数据登记。上传端点应限制并发，心跳和进度端点必须优先保持短事务。

## 迁移顺序

1. 增加 v2 Session/SeedAttempt 表和端点，不修改现有 v1 Batch 路径。
2. Worker 固定使用 Supervisor 模式，不再通过配置项切换旧 Batch 模式。
3. 前端显示“实际池/配置池”“运行 Seed/空闲槽位”和 Session 状态。
4. 在单 Worker、40 槽位、每点 30 Seed 的场景验证跨实验点补位、逐 Seed 上传、网络断连恢复和取消。
5. 验收后将默认执行模式切为动态会话；历史 BatchAttempt 保持只读直至清理窗口结束。
