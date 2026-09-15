# Worker 需求与实现约定

## 定位

Worker 是每台计算节点上的常驻 Python 服务。每个 Worker 只维护一个无头 MATLAB Supervisor 和一个本地 `parpool`，池中的每个槽位运行一个独立 Seed。Worker 不参与跨节点 MATLAB 并行。

当前 Worker 固定使用动态 Seed Session；V1 BatchAttempt 代码只保留历史数据处理，不作为新的执行入口。

## 注册和能力

Worker 使用 Join Token 注册，换取仅属于本节点的 Node Token。能力包括 Worker 版本、CPU、磁盘、MATLAB/工具箱、PlatEMO 根目录、Profile、配置池大小和实际池大小。

Profile 尚未就绪时，Worker 必须上报零容量并继续后台探测；不得领取 assignment。Profile 成功后无需重启 Worker 即可恢复接单。

## 动态执行流程

1. Worker 每 2 秒发送 V2 心跳，报告 `free_seed_slots` 和 `running_seeds`。
2. Master 在心跳响应中签发独立 Seed assignment。
3. Worker 校验 Profile、Settings、版本、磁盘和参数，写入本地 inbox。
4. MATLAB Supervisor 使用 `parfeval` 提交 Seed；提交成功后才将 inbox 文件标记为 accepted。
5. Seed 的进度事件写入 outbox，由独立 HTTP 队列上传，不阻塞 MATLAB。
6. MATLAB Future 结束后立即生成 MAT，释放计算槽位；上传和 complete 在交付队列中重试。

## MATLAB 参数和结果

`N/M/D/maxFE` 从 assignment 顶层字段传入。自定义参数只使用有序的 `algorithm_parameter_values[].value` 和 `problem_parameter_values[].value`。

每个 Seed 只生成一个 MATLAB v7 MAT，包含 `result` 和 `metric`，不写入任务元数据，不创建聚合 `result.mat`。Master 负责将其保存到实验结果目录。

## 并行池生命周期

Supervisor 创建池后尝试将支持该属性的 MATLAB 版本的 `IdleTimeout` 设置为 `Inf`。每次提交前检查池对象；若池因空闲超时或其他原因失效，自动重建池并保留尚未成功提交的 inbox 文件，不把暂时池故障报告为 Seed 永久失败。

## 心跳、交付和失效

心跳包含会话、配置池大小、实际池大小、空闲槽位及逐 Seed FE 摘要。交付阶段的 Seed 标记为 `delivering`，不占用计算槽位但继续续租。

上传产物成功后才发送 complete；所有网络失败都保留 outbox 和 MAT 并重试。收到 410 后停止该 Seed 的后续写入并保留本地审计文件，其他 Seed 不受影响。

## 取消和重启

Master 通过心跳返回 `cancel_attempt_ids`。Worker 只取消对应 Future，不关闭整个并行池。用户取消的 Seed 终态为 `cancelled`，不重新入队。

Worker 持久化 Supervisor PID、启动表达式指纹和会话目录。重启时只恢复能够验证身份的 MATLAB 进程；无法验证的进程树终止。旧 inbox 文件不能直接启动新 MATLAB 任务。

## 日志

Worker 在终端和 `Data/logs/worker.log` 记录注册、连接变化、接单、拒绝、MATLAB 启停、取消、产物上传和完成重试。正常心跳不逐条输出，不记录 Node Token 或租约密钥。
