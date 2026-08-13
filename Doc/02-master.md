# Master 与前端需求

## Master 后端能力

### PlatEMO 目录与目录解析

- 保存可访问的 PlatEMO 根路径，并验证包含 `Algorithms/`、`Problems/`、`Data/`。
- Vue 顶栏提供 PlatEMO 根目录配置入口；保存后 Master 验证目录结构并重新扫描目录、预设和已有数据测试。
- 仅纳入“文件名等于 `classdef` 类名，且继承 `ALGORITHM` 或 `PROBLEM`”的 `.m` 文件。
- 解析类文件头部的 PlatEMO 参数元数据：`% name --- default --- description`；无注释算法备用解析第一处 `ParameterSet(...)` 的变量名和默认值。
- 问题参数按 PlatEMO GUI 定义为 `N`、`M`、`D`、`maxFE` 加问题类自定义参数。M/D 可以在某些实际问题中由问题类忽略或覆盖，Master 仍保留用户的配置值和原始导入值。
- 每次目录刷新记录扫描时间、PlatEMO Git commit、可用算法/问题数量和解析异常。
- 扫描 `Data/Setting*.mat`，提供文件列表和元信息；导入一个 MAT 时，不允许执行其中任何 MATLAB 代码。
- 扫描 `Data/<Algorithm>/` 下 PlatEMO 结果命名的 MAT，聚合出可选择的已有测试 `(algorithm, problem, M, D, runs)`。

### 实验管理

- 创建、复制、保存草稿、另存为、删除、启动、停止和重试实验。
- 问题可重复加入；每个问题实例有独立参数。
- 每次保存生成不可变的实验快照，实际任务只引用快照，不引用可变 UI 状态。
- 导入 PlatEMO `Setting*.mat`，并显示参数位置不足、未知类和未消费参数等诊断；不可静默丢弃数据。
- 导出/再导入 Master 原生 `platemo-hpc-settings` MAT，包含算法实例、问题实例、运行次数、最大 Worker 数、保留点数和格式版本。该格式故意不与 PlatEMO `Setting.mat` 兼容，以支持同一问题的多参数实例。

### 任务调度与结果

- 使用数据库事务签发 Seed 批次；Worker 需要 `batch_attempt_id + lease_token` 才能更新对应 BatchAttempt。
- 接收 `result.mat`、日志、指标 CSV；校验 SHA-256 和大小后标记完成。
- 支持后置指标计算任务，避免 Worker 端因指标失败而丢失最终 MAT。
- 支持取消：Master 不再分配、向运行 Worker 发取消请求、保留中间日志。
- 当前批次调度采用 Master 分配：Master 为每个算法-问题组合创建全局 `SeedRun` 队列，不将问题绑定某个 Worker。Worker 心跳仅上报批次槽位和并行池容量；Master 在心跳响应中以事务选择兼容的待运行 Seed，签发 `batch_attempt_id + lease_token`。运行中每个 Seed 的 FE、总 FE、耗时和错误写入 `seed_runs`，批次汇总写入 `batch_attempts`，MAT/日志写入 `artifacts`。
- Worker 注册使用 `worker_join_token` 换取独立 `node_token`；Node Token 只用于该节点心跳、批次进度、完成与产物请求，UI 接口永不返回 Token。
- WatchDog 每 10 秒检查心跳；连续三次 10 秒周期无有效心跳时标记 Worker 为 `suspect`，只回收其批次内 `leased/running` 的未完成 Seed，已完成 Seed 保留，二者均记录审计事件。

## Vue 前端 UI

采用 Vue 3 Composition API、TypeScript、Pinia 和 Router。构建产物由 FastAPI 在同源 `/` 与 `/assets` 托管；开发期使用 `master/Build-Frontend.ps1` 构建，页面组件必须避免把交互放进裸 HTML 字符串。

| 页面 | 主要组件 |
| --- | --- |
| 概览 | 集群健康、在线节点、运行/排队/失败任务、近期告警 |
| 实验工作台 | `CatalogPanel`、`ExperimentEditor`、`WorkerPicker`、`ExecutionPlan` |
| 实验详情 | 参数快照、任务矩阵、实时进度、日志、产物、重试/取消 |
| Worker 管理 | 节点列表、优先级、槽位、能力标签、心跳、禁用/启用 |
| 设置文件 | 预设 MAT 列表、导入预览、另存为、版本历史 |
| 审计与告警 | WatchDog 回收、任务迁移、失败原因、操作人 |

工作台要求：左栏从上到下提供“可搜索算法、可搜索问题、执行设置、Worker 选择”；第二栏支持同问题多实例参数编辑、`Setting*.mat` 加载和原生 MAT 保存；第三栏显示运行计划和每个 Seed 的任务运行卡；第四栏显示已有数据测试候选。已有数据测试可多选以供后续结果展示；其加号操作才会按同一 M/D 创建新的问题实例。

工作台布局必须支持用户临时调整：前三列的右边界均可横向拖拽，第四列自动占据余下空间并允许较窄显示；左栏可在算法、问题、执行设置之间纵向拖拽。算法和问题区域仅有最小高度，不设置最大高度；执行设置按内容自然撑开，不允许裁切输入行。布局尺寸仅保存在当前浏览器会话；中栏顶部只保留“导入”和“保存”配置操作，不显示独立的“实验预设”选择行。

任务运行卡按 `Worker | 问题 | 算法` 显示，第二行展示 Seed 与 `N/M/D`，右侧展示 `FE / maxFE` 进度条与 ETA。原型阶段当 Worker 尚未上报 FE 时，必须显示“等待 FE 上报 / ETA 等待上报”，不得伪造进度。Master 提供只读 `GET /api/v1/ui/tasks` 供前端轮询最近任务；后续 WebSocket 接入时保持该字段模型不变。已有数据项按“问题 | 算法”显示，文件命名中缺失 N 时展示 `N—`。

成功通知（导入、保存、目录解析、创建任务）使用带进度条的浮层，约 4.6 秒后自动渐隐；错误通知保持可见，避免用户错过失败原因。

## 实时计算面板

任务行至少展示：Worker、MATLAB PID、状态、当前 FE/总 FE、进度条、已运行时间、ETA、最后日志、最后上报时间、重试次数。当前 Vue 工作台通过 `/api/v1/ui/tasks` 轮询 Seed 状态；协议稳定后可切换为 WebSocket，而不改变数据模型。

计算规则：

```text
progress = min(FE / total_FE, 1)
rate = delta_FE / delta_time
ETA = (total_FE - FE) / rate
```

当 FE 不增长、速率不足或 Worker 心跳过期时，ETA 显示“估算中/无数据/失联”，不得显示错误的有限时间。UI 更新通过 WebSocket 事件，不超过每秒一次的同 SeedRun 合并更新。

## WatchDog

- Master 每个心跳周期检查所有 BatchAttempt 租约。
- Worker 应每 10 秒上报心跳；连续 3 次未收到有效心跳（即“失联三次”），或租约过期且无法续租，则 Worker 标为 `suspect/offline`。
- 仅回收 BatchAttempt 中未确认完成的 SeedRun；若 Worker 恢复后继续上传，Master 必须拒绝过期 `lease_token` 的状态写入，结果保留为孤儿产物供人工审查。
- 回收后仅将未完成 SeedRun 置回 `pending`，创建新的 BatchAttempt，并在退避期内排除刚失联 Worker；已完成 SeedRun 不得重跑。
- 用户取消才将 SeedRun 置为 `cancelled`。Worker 进程失败、拒绝 assignment 或失联时，未完成 SeedRun 仍回到 `pending`；达到每个 Seed 的 `max_attempts` 后才置为 `failed`。
- 每次探测、回收、重新分配均记录审计事件并推送 UI 告警。
## v1 调度实现

Master 采用“Worker 主动连接、Master 主动决策”的模型：创建实验后为每个算法-问题实例创建全部 SeedRun，保存用户选中的可执行 Worker 集合。Worker 心跳报告 `available_batch_slots` 和 `max_seeds_per_batch`；Master 只在节点空闲时原子签发一个批次，并以优先级、当前占用和轮询决定下一批。10 秒一个签名心跳，连续三次缺失会回收该批次尚未完成的 Seed。接口细节和实现清单见 `Doc/07-seed-batch-scheduling.md`。
