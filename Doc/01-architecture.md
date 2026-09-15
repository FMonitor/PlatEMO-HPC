# 总体架构与数据模型

## 拓扑

```text
浏览器 Vue 前端
        │ HTTP
        ▼
Master FastAPI + 调度器 + SQLite
        │ HTTP/ZeroTier，Worker 主动心跳
        ├── Worker A：Python + MATLAB Supervisor + 本地 parpool
        ├── Worker B：Python + MATLAB Supervisor + 本地 parpool
        └── Worker C：Python + MATLAB Supervisor + 本地 parpool
```

Worker 不接受 Master 的入站任务连接。Worker 周期性发送心跳，Master 在心跳响应中返回 Seed assignment。

## 任务层级

```text
Experiment
  └─ ExperimentPoint = 一个算法实例 + 一个问题实例
       └─ SeedRun = 一个随机种子的一次独立运行
            └─ SeedAttempt = 该 Seed 当前一次租约执行
```

`SeedRun` 是调度和重试的最小单位。一个 Seed 失联或失败后，可以创建新的 `SeedAttempt`，已完成的 Seed 不会重复运行。

## 主要数据表

| 表 | 用途 |
| --- | --- |
| `experiments` | 一次提交的实验及全局配置 |
| `experiment_points` | 算法、问题及参数快照 |
| `seed_runs_v2` | 全局 Seed 队列、当前状态、FE 和结果路径 |
| `seed_attempts_v3` | V2 动态 Seed 的租约、Worker、会话和错误 |
| `worker_sessions_v2` | Worker 当前 MATLAB 会话和池容量 |
| `artifacts_v3` | Seed MAT 的固定产物 ID、哈希和保存路径 |
| `workers` | 节点注册、心跳、能力和暂停接单状态 |
| `events` | 调度、回收、取消和用户操作审计 |

旧的 BatchAttempt 表只用于历史记录或兼容读取，不是当前动态调度入口。

## Seed 状态

```text
pending → leased → running → completed
   │         │          ├── failed
   │         │          └── cancelled
   └─────────┴── WatchDog 回收后回到 pending
```

产物上传成功但完成确认尚未成功时，Worker 本地处于 `delivering`；Master 仍保持租约，只有产物和 complete 都确认后才进入 `completed`。

## 调度原则

1. Master 创建实验时为每个 ExperimentPoint 生成全部 pending SeedRun，不绑定特定 Worker。
2. Worker 通过心跳报告 `free_seed_slots`、配置池大小和实际池大小。
3. Master 在同一个 SQLite 写事务中选择兼容、未暂停且在线 Worker 的 pending Seed，并生成唯一 `attempt_id + lease_token`。
4. 一个 Seed 同一时刻只能有一个有效租约。
5. Worker 交付不占用 MATLAB 池槽位，因此完成一个 Seed 后可以立即领取下一个 Seed。
6. Worker 暂停接单只阻止新 assignment，不停止已经运行或交付中的 Seed。
