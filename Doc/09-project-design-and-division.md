# PlatEMO-HPC 项目设计与分工说明

## 1. 项目背景与目标

PlatEMO 是基于 MATLAB 的多目标优化实验平台。传统实验方式通常依赖 MATLAB Parallel Computing Toolbox 的并行池或 Parallel Server 许可证，任务编排、节点状态监控、失败重试和结果收集能力有限，不适合多台异构个人计算设备组成的小型集群。

本项目的目标是建设一个基于 ZeroTier 网络的轻量级 HPC 调度平台：

- 使用浏览器统一配置算法、问题、参数、运行次数和结果保留策略。
- 将每个随机种子作为独立调度单位，支持跨算法、跨问题和跨 Worker 动态补位。
- 每台 Worker 只维护一个 MATLAB Supervisor 和一个本地 `parpool`，不依赖 MATLAB Parallel Server 许可证。
- 实时显示 Worker 状态、实际并行池容量、Seed 的 FE/MaxFE、耗时和 ETA。
- 在网络中断、MATLAB 崩溃、Worker 重启和租约过期时自动回收并重新调度未完成 Seed。
- 确保 MAT 结果具备固定归属、SHA-256 校验、幂等上传和可审计事件记录。

当前拓扑为：

```text
浏览器 Vue
    │ HTTP
    ▼
Master：FastAPI + SQLite + 调度器 + WatchDog
    │ ZeroTier / HTTP 心跳响应
    ├── Worker：Python + MATLAB Supervisor + local parpool
    ├── Worker：Python + MATLAB Supervisor + local parpool
    └── Worker：Python + MATLAB Supervisor + local parpool
```

## 2. 技术与 AI 工具选择

### 2.1 运行技术

| 层次 | 技术 | 选择原因 |
| --- | --- | --- |
| 前端 | Vue 3 + TypeScript + Vite | 组件化适合调度面板，类型检查和构建速度较好，易于实现实时状态、分页、拖拽布局和弹窗交互。 |
| Master API | Python + FastAPI | 异步 HTTP、类型声明和接口文档能力较强，适合心跳、控制接口和文件上传。 |
| Master 存储 | SQLite | 部署简单、事务可靠，适合个人集群；通过短事务保护租约和状态迁移。 |
| Worker | Python | 便于实现常驻服务、队列、进程监控、HTTP 重试和 Windows 进程树管理。 |
| MATLAB 执行 | MATLAB `-batch` + Supervisor + `parpool`/`parfeval` | 直接使用 PlatEMO 的真实调用路径，避免虚构适配层；一次启动池后复用计算资源。 |
| 网络 | ZeroTier + HTTP | 解决异构设备互联问题，接口协议清晰，调试和部署成本低。 |

### 2.2 AI 模型与工具的选择

项目采用 Codex 类编码模型作为主要工程助手，原因是本项目同时包含 Python、TypeScript、MATLAB、PowerShell、SQLite 和协议文档，需要跨文件理解、局部修改、测试和持续审查。

AI 工具的使用原则如下：

1. 先读取仓库、协议和运行日志，再提出修改；不凭接口名称猜测 MATLAB 或 Worker 行为。
2. 对协议、租约、取消、产物幂等等高风险逻辑，使用代码审查和回归测试验证，而不是仅依赖模型生成结果。
3. 对前端改动执行 TypeScript 检查和 Vite 构建；对 Python 执行语法检查和接口级测试。
4. 真实 MATLAB 联调以本机 PlatEMO、MATLAB Profile 和生成的 `result.mat` 为最终依据。
5. AI 不拥有最终运行批准权；涉及删除数据、终止进程、覆盖结果或长时间实验时，必须保留人工确认边界。

## 3. AI 会话分工

### 3.1 底座会话：项目架构与公共约定

由一个独立会话负责项目底座，职责包括：

- 建立仓库、目录结构、启动脚本和开发环境说明。
- 编写 `Doc/01` 至 `Doc/09` 等架构、部署、协议和调度文档。
- 定义 Experiment、ExperimentPoint、SeedRun、SeedAttempt、Artifact 等数据模型。
- 定义 V2 API、状态机、租约语义、取消语义、错误码和兼容策略。
- 提供测试夹具、数据库初始化和最小端到端验证方式。
- 维护变更记录，确保实现和文档同步。

底座会话不直接替代 Master 或 Worker 的业务实现，而是提供两边共同遵守的可验证契约。

### 3.2 Master 会话

Master 会话专注于 `master/`，负责：

- FastAPI API、SQLite 事务和 WatchDog。
- PlatEMO 算法/问题/Settings 解析和实验快照创建。
- 动态 Seed 调度、Worker 优先级、容量扣减和兼容性筛选。
- V2 heartbeat、assignment、progress、artifact、complete 和 cancel 接口。
- 租约过期、重复请求、旧 token、产物缺失和异常 Worker 的防护。
- Vue/TypeScript 前端，包括任务配置、Worker 管理、实时进度和历史记录。
- Master 自身的单元测试、协议测试、语法检查和前端构建。

Master 会话必须把无法由 Worker 解决的前置条件写入 assignment，例如 Profile、PlatEMO commit、Settings 摘要和磁盘下限。

### 3.3 Worker 会话

Worker 会话专注于 `worker/`，负责：

- Worker 注册、能力探测、MATLAB Profile 探测和本地 Session 持久化。
- 维护一个 MATLAB Supervisor 和一个本地并行池。
- 通过 `parfeval` 为独立 Seed 提交 PlatEMO 任务。
- 采集 FE、总 FE、耗时、PID、池容量和 Seed 状态。
- 维护 inbox/outbox，保证网络失败时任务和产物不会丢失。
- 按 attempt/token 上传进度和 MAT 产物，并正确处理 HTTP 410。
- 执行取消命令，仅终止指定 Future，不影响其它 Seed 或整个并行池。
- 处理 MATLAB 崩溃、池重建、Worker 重启和遗留进程校验。

Worker 会话不得自行扩展 Master 的状态含义；遇到不兼容 assignment 必须使用约定错误码拒绝，并等待 Master 退避或重新调度。

### 3.4 互相校验机制

Master 和 Worker 会话分别实现后，按照以下顺序互检：

1. 以 `Doc/04-api-protocol.md` 和 `Doc/08-dynamic-seed-session.md` 为唯一协议基线。
2. Master 使用模拟 Worker 检查注册、心跳、分配、进度、产物、完成、取消、回收和重试。
3. Worker 使用模拟 Master 检查 accepted/rejected、410、断网重试、取消和重启恢复。
4. 双方检查每个请求是否同时携带并校验 `attempt_id`、`lease_token` 和 `experiment_point_id`。
5. 使用真实 MATLAB 做单 Worker 小规模任务，再做多槽位、多问题、多算法和长任务验证。
6. 任何一方发现接口假设不一致，先修改文档和测试，再修改实现；不通过保留旧接口掩盖不一致。

## 4. 核心挑战与解决方案

### 4.1 没有 Parallel Server 许可证

解决方案是把跨节点调度和节点内并行分离：Master 只调度 Seed，Worker 在本地使用一个 MATLAB `parpool`，每个池槽位运行一个 Seed。这样不依赖 MATLAB Parallel Server 的跨节点调度能力。

### 4.2 异构设备和动态容量

Worker 上报配置池大小、实际池大小、空闲 Seed 槽位、Profile、版本和磁盘信息。Master 在事务内计算可用容量，并扣除已有有效租约，避免旧心跳或异常声明导致重复超售。

### 4.3 长任务、断网和失联回收

每个 Seed 使用独立租约。heartbeat 和 progress 在租约有效时续租；过期请求返回 410。WatchDog 回收未完成 Seed，已确认有产物的 Seed 不重复计算。Worker 使用持久 outbox，网络恢复后继续上传。

### 4.4 结果完整性和幂等

产物使用固定 artifact ID、逻辑归属、文件大小和 SHA-256。Master 先完成临时文件写入和校验，再在短事务中登记元数据；同一逻辑产物相同摘要可重试，不同内容返回冲突。

### 4.5 MATLAB 生命周期

Worker 不为每个 Seed 启动独立 MATLAB。Supervisor 负责池探测、重建、Future 管理、PID 校验和取消。池故障与 Seed 算法失败分开处理，避免暂时性池故障被错误记录为永久失败。

### 4.6 前端状态复杂度

前端将计划分配、活动 Seed、任务记录和已有数据分区显示，实时刷新 Worker/Seed 状态，使用分页、筛选、拖拽列宽和加载遮罩降低误操作风险。取消、暂停、停止、删除和刷新均通过气泡反馈结果。

## 5. 项目结果

目前已形成以下可交付成果：

- Master/Worker 双项目结构、PowerShell 启动脚本和 ZeroTier 部署方式。
- Vue 3 专业调度面板：PlatEMO 目录解析、算法/问题参数配置、Worker 管理、任务分配、Seed 进度、ETA 和任务记录。
- V2 动态 Seed 协议，退役旧 V1 Worker 执行接口。
- 独立 Seed 租约、心跳续租、410 失效、WatchDog 回收、取消和重试机制。
- MATLAB Supervisor + 本地 `parpool` + `parfeval` 执行模型。
- MAT 产物固定归属、SHA-256 校验、幂等上传和本地 outbox 交付。
- Settings、PlatEMO commit、Profile、磁盘和 Worker 能力的环境约束传递。
- Master 协议测试、Worker 测试、Python 语法检查和 Vue 构建流程。
- 已完成真实 MATLAB 小规模运行验证：Worker 能通过 PlatEMO 真实调用生成可加载的 `result.mat`，进度可达到设定的 FE/MaxFE。

## 6. 当前边界与后续工作

当前系统定位为个人/小型实验集群，不等同于商业 HPC 调度器。后续重点包括：

- 完善真实长任务、多 Worker 断网恢复和 MATLAB 崩溃联合测试。
- 将 Worker 优先级、轮询、公平性和确定性不兼容退避纳入可观测指标。
- 增加任务日志、实验级统计、指标计算和结果下载体验。
- 评估 SQLite 到 PostgreSQL 的迁移边界，但保持现有租约和状态机语义不变。
- 为 Master、Worker 和协议分别建立持续集成检查，避免文档、前端和执行端再次发生漂移。
