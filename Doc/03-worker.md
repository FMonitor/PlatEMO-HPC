# Worker 需求

## 定位

Worker 是每台计算节点的常驻 Python 服务。它只执行本机 MATLAB 和本机 `parpool`；不尝试加入跨节点 MATLAB 并行池。

当前实现以 `execution_mode: dynamic_seed_session` 为新协议入口：一个 Worker 维护一个持久 MATLAB Supervisor/parpool，Pool 的每个空闲槽位可领取任意兼容实验点的单个 Seed。本文后续的 BatchAttempt 段落仅适用于迁移期间保留的旧模式；动态模式以 [08-dynamic-seed-session.md](08-dynamic-seed-session.md) 为准。

## 注册和能力声明

首次启动使用一次性注册令牌注册。之后由 Worker 自己用节点密钥认证。注册数据包括：

- Worker 名称、ZeroTier URL、版本。
- CPU 逻辑核数、可配置本地槽位、内存、GPU 设备和显存。
- MATLAB 版本、Parallel Computing Toolbox 是否可用、可用本地 pool 上限。
- PlatEMO 根目录、Git commit、数据文件哈希、可用算法/问题清单摘要。
- 手工优先级由 Master 保存，Worker 不可自行提高。

## 执行流程

1. Worker 心跳上报空闲批次槽位和并行池容量，接收 Master 在响应中分配的一个 Seed 批次。
2. 校验 lease token、本机 MATLAB cluster profile 与 assignment 完全一致、PlatEMO commit、Settings MAT、磁盘空间与本地槽位；profile 不匹配必须以 `profile_unavailable` 拒绝，不得启动 MATLAB。`N/M/D/max_fe` 必须显式传给 PlatEMO；问题自定义参数只能采用有序 `problem_parameter_values[].value` 作为 `parameter` 传入，不能依赖 JSON 字段顺序。
3. 创建隔离工作目录：`Data/work/<experiment_point_id>/<batch_attempt_id>/`。
4. 启动 `matlab.exe -batch` 调用统一的 `run_task.m`。
5. `run_task.m` 根据 Master assignment 中的参数设置算法/问题，仅运行 assignment 指定的 Seed 批次。
6. 定期写 `progress.json`；Worker 读取并上报。
7. 结束后校验每个已完成 Seed 的 `runs/seed-<seed>.mat`，逐个上传结果、哈希和退出码；不创建或上传批次聚合 `result.mat`。
8. 成功/失败后释放本地批次槽位；下一次心跳由 Master 决定是否分配新批次。收到 `410` 时必须立即终止 MATLAB 进程树，禁止继续上传产物或完成确认。

## MATLAB 包装器约定

`run_task.m(taskJsonPath, workDir)` 必须：

- 不依赖 GUI，不调用 MATLAB Parallel Server。
- 读取算法名、算法参数、问题名、问题参数、seed、maxFE、Settings MAT 和保留数据点数。
- 可选启动本机 `parpool("Processes", localPoolSize)`；必须在结束时清理本任务创建的 pool。
- 至少每 `progress_interval_fe` 写入一次 JSON：`FE`、`totalFE`、`phase`、`timestamp`。
- 保留的过程数据不得超过任务设置的 `retain_points`。调用 `platemo` 时不得请求返回值，否则 PlatEMO 会强制 `save=0`；Worker 的 output callback 必须在完成时保存 `Algorithm.result` 和 `Algorithm.metric`，使每个 `seed-*.mat` 保留该数量的过程快照。
- 在每个 `runs/seed-<seed>.mat` 中记录任务快照、算法/问题参数、seed、MATLAB 版本和结束状态。

## 心跳和进度

Worker 启动时必须在后台探测 MATLAB cluster profile，不能因为首次 MATLAB 进程尚未就绪而阻塞注册或心跳。探测未成功期间持续上报在线、`configured_pool_workers=0`、`max_seeds_per_batch=0` 和 `available_batch_slots=0`；每 30 秒自动重试。单次探测由 `profile_probe_timeout_seconds` 控制，默认 300 秒、允许 45 至 600 秒，以适应冷启动 MATLAB。探测到正数 `NumWorkers` 后自动恢复可接单容量，不需要重启 Worker。

Worker 每 10 秒上报一次。空闲时必须包含 `available_batch_slots`、`max_concurrent_batches`、`configured_pool_workers`、`max_seeds_per_batch`；若无法读取或验证 MATLAB profile，`configured_pool_workers`、`max_seeds_per_batch` 和 `available_batch_slots` 必须为 `0`，并且不得接收 assignment。忙碌 Worker 还必须包含每个 BatchAttempt 的：

- `experiment_point_id`、`batch_attempt_id`、`lease_token`、PID 与该批次的 Seed 列表。
- FE、total FE、进度、ETA 输入数据、运行时间。
- 最近日志尾部或日志 offset。
- 本地槽位占用、CPU、内存、GPU 利用率。

一个 Worker 同时只运行一个 MATLAB 批次和一个本地 `parpool`。不允许在运行中的池追加另一个 ExperimentPoint 的 Seed。启动 MATLAB 后 Worker 必须在工作目录持久化 `process.json`，记录 PID、命令指纹、lease 和 task 路径。服务重启时，只能对 batch ID、lease token、task 路径和 MATLAB `-batch` 启动表达式 SHA-256 均一致的存活 PID 恢复心跳/槽位占用，绝不重新启动 MATLAB；恢复期间继续读取 `progress.json` 并调用 progress API，以更新每个 Seed 的 FE、ETA 和 pool 状态。无法验证的遗留 MATLAB PID 必须终止进程树，保留目录等待 Master 回收。收到 Master 取消或 `410` 后，Worker 必须以 `taskkill /T /F` 终止当前或恢复 PID 的 MATLAB 树，并在最终 progress/complete 中将未终态 Seed 统一报告为 `cancelled`；`completed` 和 `failed` Seed 保持原终态。MATLAB 结束后，Worker 必须先持久化 `summary.json` 与 `delivery.json`，以每个 Seed 固定 artifact ID 重试 `PUT artifact`，并且只在每个 completed Seed 的产物成功登记后重试 `complete`；若 MATLAB 失败且没有可恢复的 Seed MAT，Worker 必须直接报告 `failed` 完成并在 Master 接受后释放槽位，Master 会把未由产物覆盖的 completed Seed 重新入队。任一临时通信失败都继续心跳续租并保留本地槽位，不得释放为可分配状态或重新启动 MATLAB。交付期间必须报告 `phase=delivering`、MATLAB PID 为 `0`、实际 pool workers 为 `0`，不得伪装成仍在计算。重启后若 `running/<batch_attempt_id>.json` 对应工作目录已存在 `summary.json` 和 `delivery.json`，可仅恢复产物/完成交付，不得重跑 MATLAB。其余无法确认的队列和工作目录必须保留，等待 Master 回收。交付确认后 Worker 可删除 transient `running/<batch_attempt_id>.json`，但必须保留 `work/<experiment_point_id>/<batch_attempt_id>/task.json`、日志和结果以供审计。

assignment 可携带 `settings_file`、`settings_sha256`、`settings_download_url`、`required_platemo_commit` 和 `minimum_disk_free_bytes` 等环境约束。`settings_download_url` 存在时，Worker 使用 Node Token 下载到自身 `PlatEMO/Data`，只在 SHA-256 匹配后才继续校验并 accepted；没有下载地址时，Settings MAT 必须已位于自身 PlatEMO 根目录或 `Data` 目录且可读。Worker 同时校验 PlatEMO commit 和可用磁盘；不满足时以稳定错误码 `input_incompatible`、`version_mismatch` 或 `insufficient_disk` 拒绝。

## 本地 WatchDog 和安全

- Worker 应杀死超过 Master 取消截止时间的 MATLAB 子进程，并上报 `cancelled`。
- 不执行 Master 任意传入的 shell 命令；只接受定义明确的任务 JSON。
- 只允许 Master 指定白名单下的 PlatEMO 文件、Settings MAT 和结果路径。
- API token/节点密钥只存放在 `config.json` 或受保护的系统凭据中，日志不得打印密钥。

## 运行日志

Worker 必须同时向启动终端和 `Data/logs/worker.log` 输出生命周期日志；文件使用 UTF-8、单文件最大 2 MiB、保留最近 5 个轮转文件。日志记录注册、Master 连接断开/恢复、assignment 接受或拒绝、MATLAB 启动 PID、MATLAB 退出、取消、产物上传和完成确认/重试。不得逐次输出正常心跳，也不得记录 node token、lease token 或其他密钥。每个 BatchAttempt 的 MATLAB 标准输出仍保留在其工作目录 `matlab.log`，结构化状态仍以 `task.json`、`progress.json`、`summary.json` 与 `delivery.json` 为准。每个成功 Seed 的 `runs/seed-<seed>.mat` 单独上传；Master 按 `Data/experiments/<experiment_id>/<algorithm>/` 保存同一 Experiment 的结果，文件名为 `<algorithm>_<problem>_M<M>_D<D>_<seed>.mat`，便于将算法目录迁移到 PlatEMO 的 `Data/<algorithm>/`。

## 补充约定

- `settings_file` 是兼容性基线，不是参数覆盖源：Worker 只验证其 SHA-256、算法名和问题名，并在每个 Seed MAT 的任务快照中记录已使用文件；算法、问题、`N/M/D/max_fe` 与有序参数值始终以 Master assignment 为准。若未来需要让 Settings 中其他字段生效，必须先在协议中定义字段白名单和合并优先级。
- `running_batches` 必须保留每个 Seed 最近一次的 `state`、`fe`、`total_fe`、`elapsed_seconds` 与由这些数据计算的 `eta_seconds`，以及批次池状态和 PID。这样进度请求短暂丢失时，Master 仍能从下一次心跳重建运行视图。
