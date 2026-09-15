# PlatEMO-HPC 项目文档

本文档描述当前可运行的 Master-Worker 实现。当前协议以动态 Seed 调度为唯一运行协议：Master 管理全局 Seed 队列，Worker 使用本机 MATLAB 并行池的空闲槽位逐个领取 Seed。

## 文档索引

| 文件 | 内容 |
| --- | --- |
| [01-architecture.md](01-architecture.md) | 系统拓扑、数据模型和状态流转 |
| [02-master.md](02-master.md) | Master 后端、调度器和前端职责 |
| [03-worker.md](03-worker.md) | Worker、MATLAB Supervisor 和故障恢复 |
| [04-api-protocol.md](04-api-protocol.md) | 当前 V2 HTTP 接口和数据格式 |
| [05-worker-deployment.md](05-worker-deployment.md) | Worker 安装、配置和启动 |
| [06-task-process-control.md](06-task-process-control.md) | Seed 级取消、租约和进程安全 |
| [07-seed-batch-scheduling.md](07-seed-batch-scheduling.md) | 全局 Seed 调度规则和验收要求 |
| [08-dynamic-seed-session.md](08-dynamic-seed-session.md) | 动态会话的 MATLAB 文件队列和交付流程 |

## 当前边界

- 每个 Worker 只维护一个无头 MATLAB Supervisor 和一个本地 `parpool`。
- 每个并行池槽位运行一个独立 Seed；不同问题点和算法可以混合调度。
- 不使用跨机器 MATLAB 并行池、MATLAB Parallel Server、MPI 或 Kubernetes。
- Master 的持久化数据库当前为 `master/Data/master.sqlite3`。
- V1 Worker 接口已经退役。Master 对 V1 心跳、BatchAttempt 进度、完成和产物接口统一返回 `410 protocol_v1_retired`。

## 结果文件

每个完成 Seed 上传一个传统 MATLAB v7 MAT 文件，包含 `result` 和 `metric` 变量。Master 保存为：

```text
Data/experiments/<experiment_id>/<algorithm>/
  <algorithm>_<problem>_M<M>_D<D>_<seed>.mat
```

`result` 保持 PlatEMO 的传统结果格式；任务元数据、租约和错误信息保存在 SQLite 与 JSON 审计文件中，不写入结果 MAT。
