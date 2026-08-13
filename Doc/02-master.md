# Master 与前端需求

## Master 后端能力

### PlatEMO 目录与目录解析

- 保存可访问的 PlatEMO 根路径，并验证包含 `Algorithms/`、`Problems/`、`Data/`。
- 仅纳入“文件名等于 `classdef` 类名，且继承 `ALGORITHM` 或 `PROBLEM`”的 `.m` 文件。
- 解析类文件头部的 PlatEMO 参数元数据：`% name --- default --- description`；备用解析 `ParameterSet(...)` 和 `Setting` 中的 M/D 默认值。
- 每次目录刷新记录扫描时间、PlatEMO Git commit、可用算法/问题数量和解析异常。
- 扫描 `Data/Settings*.mat`，提供文件列表和元信息；导入一个 MAT 时，不允许执行其中任何 MATLAB 代码。

### 实验管理

- 创建、复制、保存草稿、另存为、删除、启动、停止和重试实验。
- 问题可重复加入；每个问题实例有独立参数。
- 每次保存生成不可变的实验快照，实际任务只引用快照，不引用可变 UI 状态。
- 导入/导出标准 `settings.mat`。导出至少包含算法实例、问题实例、运行次数、最大 Worker 数、保留点数和 UI 版本。
- 对旧 PlatEMO Settings MAT：显示可导入字段和无法识别字段；不可静默丢弃数据。

### 任务调度与结果

- 使用数据库事务领取任务；Worker 需要 `task_id + lease_token` 才能更新对应 Attempt。
- 接收 `result.mat`、日志、指标 CSV；校验 SHA-256 和大小后标记完成。
- 支持后置指标计算任务，避免 Worker 端因指标失败而丢失最终 MAT。
- 支持取消：Master 不再分配、向运行 Worker 发取消请求、保留中间日志。

## Vue 前端 UI

采用 Vue 3 Composition API、TypeScript、Pinia 和 Router。页面组件必须避免把交互放进裸 HTML 字符串。

| 页面 | 主要组件 |
| --- | --- |
| 概览 | 集群健康、在线节点、运行/排队/失败任务、近期告警 |
| 实验工作台 | `CatalogPanel`、`ExperimentEditor`、`WorkerPicker`、`ExecutionPlan` |
| 实验详情 | 参数快照、任务矩阵、实时进度、日志、产物、重试/取消 |
| Worker 管理 | 节点列表、优先级、槽位、能力标签、心跳、禁用/启用 |
| 设置文件 | 预设 MAT 列表、导入预览、另存为、版本历史 |
| 审计与告警 | WatchDog 回收、任务迁移、失败原因、操作人 |

工作台要求：左栏提供可搜索算法/问题；中栏支持多实例参数编辑、Settings 加载/保存与 Worker 勾选；右栏显示运行计划、预计任务数、实时事件和开始/停止按钮。

## 实时计算面板

任务行至少展示：Worker、MATLAB PID、状态、当前 FE/总 FE、进度条、已运行时间、ETA、最后日志、最后上报时间、重试次数。

计算规则：

```text
progress = min(FE / total_FE, 1)
rate = delta_FE / delta_time
ETA = (total_FE - FE) / rate
```

当 FE 不增长、速率不足或 Worker 心跳过期时，ETA 显示“估算中/无数据/失联”，不得显示错误的有限时间。UI 更新通过 WebSocket 事件，不超过每秒一次的同 Task 合并更新。

## WatchDog

- Master 每个心跳周期检查所有租约中的 Task。
- Worker 应每 10 秒上报心跳；连续 3 次未收到有效心跳（即“失联三次”），或租约过期且无法续租，则 Worker 标为 `suspect/offline`。
- 仅回收未确认完成的 Task；若 Worker 恢复后继续上传，Master 必须拒绝过期 `lease_token` 的状态写入，结果保留为孤儿产物供人工审查。
- 回收后将 Task 置回 `queued`，`attempt_no + 1`，排除刚失联 Worker，按重试策略投递其它节点。
- 达到 `max_attempts` 后置 `failed`，不无限重试。
- 每次探测、回收、重新分配均记录审计事件并推送 UI 告警。
