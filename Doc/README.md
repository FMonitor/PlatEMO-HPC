# PlatEMO HPC v2 需求文档

本文档组定义 PlatEMO HPC 从当前原型演进为个人异构集群调度平台的目标能力。当前优先场景是 PlatEMO 的多算法、多问题、多随机种子实验，Worker 在本机启动 MATLAB 和本地 `parpool`；不依赖 MATLAB Parallel Server。

## 文档索引

| 文档 | 内容 |
| --- | --- |
| [01-architecture.md](01-architecture.md) | 技术选型、总体架构、数据模型和运行状态机 |
| [02-master.md](02-master.md) | Master 后端、Vue 管理面板、PlatEMO 设置和 WatchDog 需求 |
| [03-worker.md](03-worker.md) | Worker 执行器、MATLAB 包装器、进度与恢复需求 |
| [04-api-protocol.md](04-api-protocol.md) | REST/WebSocket 通信协议、认证、幂等和错误语义 |
| [07-seed-batch-scheduling.md](07-seed-batch-scheduling.md) | 多 Worker 分担同一实验点的 Master 分配方案、实现边界和验收条件 |

## 目标与非目标

目标：在 ZeroTier 私有网络中可靠分配 PlatEMO 实验，实时显示任务的 FE/总 FE、ETA、日志和结果；Worker 失联后自动回收未完成任务；依据节点优先级和资源能力调度。

非目标：第一阶段不实现跨机器 MATLAB `parpool`、MATLAB Parallel Server、跨节点 MPI/NCCL 训练或 Kubernetes。每个 Worker 仅使用本机 MATLAB 和本机并行池。

## 设置文件兼容性

- **导入**：兼容 PlatEMO `Data/Setting*.mat`。其原生变量是 `Setting={algorithms,problems,flatParameters}` 与 `Environment=[runs,retainResults]`；Master 仅解析 MAT 数据，不执行其中任何 MATLAB 代码。
- **导出**：保存为 Master 原生 MAT，包含 `PlatEMO_HPC_FormatVersion` 和 JSON 配置。它可完整保留同一问题的多个不同参数实例，**刻意不兼容** PlatEMO GUI 的 `Setting.mat` 格式。
- 因而“导出的 m 文件”在本文档中按设置 MAT 文件理解；本项目不会生成或执行动态 `.m` 配置脚本。

## 建议技术栈

| 层 | 建议 | 原因 |
| --- | --- | --- |
| 前端 | Vue 3、Vite、TypeScript、Pinia、Vue Router、ECharts | 可维护多面板状态、可复用任务/节点组件、适合实时事件流 |
| Master API | Python 3.11+、FastAPI、Pydantic、SQLAlchemy/Alembic | 现有 Python 原型可平滑迁移，适合科学数据、MAT 文件和异步 API |
| 状态数据库 | PostgreSQL；开发期可 SQLite | 任务领取、租约、重试和审计需要事务；生产多进程不应继续使用 SQLite |
| 实时通道 | WebSocket（Master -> UI）；Worker HTTP 上报或 Worker WebSocket（Worker -> Master） | UI 不轮询；Worker 事件可快速传播 |
| 队列/锁 | PostgreSQL 行锁起步；可选 Redis | 保证只会领取一次、失联后可安全回收 |
| Worker | Python 服务、MATLAB `-batch`、PowerShell 服务包装 | Windows/PlatEMO 环境直接兼容 |
| 网络 | ZeroTier、HTTPS 或 ZeroTier 内 HTTP + Token | 节点互通且保持私网边界 |

## 优先实现顺序

1. 规范化数据库、Worker 注册、Master Seed 批次分配和幂等 API。
2. Worker 执行 MATLAB 包装器，上报 FE、总 FE、ETA、日志和最终 MAT。
3. Vue 管理面板与 WebSocket 实时状态。
4. WatchDog、重试、优先级和审计界面。
5. Settings MAT 的完整导入导出和结果指标计算。
