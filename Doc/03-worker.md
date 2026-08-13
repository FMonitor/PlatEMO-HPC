# Worker 需求

## 定位

Worker 是每台计算节点的常驻 Python 服务。它只执行本机 MATLAB 和本机 `parpool`；不尝试加入跨节点 MATLAB 并行池。

## 注册和能力声明

首次启动使用一次性注册令牌注册。之后由 Worker 自己用节点密钥认证。注册数据包括：

- Worker 名称、ZeroTier URL、版本。
- CPU 逻辑核数、可配置本地槽位、内存、GPU 设备和显存。
- MATLAB 版本、Parallel Computing Toolbox 是否可用、可用本地 pool 上限。
- PlatEMO 根目录、Git commit、数据文件哈希、可用算法/问题清单摘要。
- 手工优先级由 Master 保存，Worker 不可自行提高。

## 执行流程

1. Worker 心跳上报空闲批次槽位和并行池容量，接收 Master 在响应中分配的一个 Seed 批次。
2. 校验 lease token、本机 MATLAB cluster profile 与 assignment 完全一致、PlatEMO commit、Settings MAT、磁盘空间与本地槽位；profile 不匹配必须以 `profile_unavailable` 拒绝，不得启动 MATLAB。
3. 创建隔离工作目录：`Data/work/<experiment_point_id>/<batch_attempt_id>/`。
4. 启动 `matlab.exe -batch` 调用统一的 `run_task.m`。
5. `run_task.m` 根据 Master assignment 中的参数设置算法/问题，仅运行 assignment 指定的 Seed 批次。
6. 定期写 `progress.json`；Worker 读取并上报。
7. 结束后校验 `result.mat`，上传结果、日志、哈希和退出码。
8. 成功/失败后释放本地批次槽位；下一次心跳由 Master 决定是否分配新批次。收到 `410` 时必须立即终止 MATLAB 进程树，禁止继续上传产物或完成确认。

## MATLAB 包装器约定

`run_task.m(taskJsonPath, workDir)` 必须：

- 不依赖 GUI，不调用 MATLAB Parallel Server。
- 读取算法名、算法参数、问题名、问题参数、seed、maxFE、Settings MAT 和保留数据点数。
- 可选启动本机 `parpool("Processes", localPoolSize)`；必须在结束时清理本任务创建的 pool。
- 至少每 `progress_interval_fe` 写入一次 JSON：`FE`、`totalFE`、`phase`、`timestamp`。
- 保留的过程数据不得超过任务设置的 `retain_points`。
- 在 `result.mat` 中记录任务快照、算法/问题参数、seed、MATLAB 版本、PlatEMO commit 和结束状态。

## 心跳和进度

Worker 每 10 秒上报一次。空闲时必须包含 `available_batch_slots`、`max_concurrent_batches`、`configured_pool_workers`、`max_seeds_per_batch`；若无法读取或验证 MATLAB profile，`configured_pool_workers`、`max_seeds_per_batch` 和 `available_batch_slots` 必须为 `0`，并且不得接收 assignment。忙碌 Worker 还必须包含每个 BatchAttempt 的：

- `experiment_point_id`、`batch_attempt_id`、`lease_token`、PID 与该批次的 Seed 列表。
- FE、total FE、进度、ETA 输入数据、运行时间。
- 最近日志尾部或日志 offset。
- 本地槽位占用、CPU、内存、GPU 利用率。

一个 Worker 同时只运行一个 MATLAB 批次和一个本地 `parpool`。不允许在运行中的池追加另一个 ExperimentPoint 的 Seed。收到 Master 取消后，Worker 终止 MATLAB，并在最终 progress/complete 中将未终态 Seed 统一报告为 `cancelled`；`completed` 和 `failed` Seed 保持原终态。重启后不得执行本地遗留的 assignment JSON；只有能通过 PID 身份确认的存活 BatchAttempt 才可重新上报，无法确认的队列和工作目录必须保留，等待 Master 回收。Worker 可删除 transient `running/<batch_attempt_id>.json`，但必须保留 `work/<experiment_point_id>/<batch_attempt_id>/task.json`、日志和结果以供审计。

当前 Master 尚未下发 `settings_file`、`required_platemo_commit` 和 `minimum_disk_free_bytes` 等环境约束。两端补充这些 assignment 字段后，Worker 必须在发送 `accepted` 前校验：Settings MAT 位于允许路径且可读、PlatEMO commit 一致、可用磁盘不低于下限；不满足时以稳定错误码 `input_incompatible`、`version_mismatch` 或 `insufficient_disk` 拒绝。

## 本地 WatchDog 和安全

- Worker 应杀死超过 Master 取消截止时间的 MATLAB 子进程，并上报 `cancelled`。
- 不执行 Master 任意传入的 shell 命令；只接受定义明确的任务 JSON。
- 只允许 Master 指定白名单下的 PlatEMO 文件、Settings MAT 和结果路径。
- API token/节点密钥只存放在 `config.json` 或受保护的系统凭据中，日志不得打印密钥。
