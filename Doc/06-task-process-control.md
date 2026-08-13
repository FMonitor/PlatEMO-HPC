# 批次进程控制约定

一个 Worker 只启动一个 MATLAB 批次进程。该进程按任务的 `cluster_profile` 创建 MATLAB `parpool`，并使用 `parfor` 执行多个 seed。

停止操作只针对批次：Master 后续通过 `/api/v1/tasks/{task_id}/cancel` 标记取消并由 Worker 心跳领取取消指令，终止 MATLAB 进程树并关闭该批次的并行池。不能单独终止 pool worker，也不要求 seed 级强制停止。

每个 seed 有独立结果文件和进度记录。`progress.json` 至少包含 `completed_runs`、`running_runs`、`failed_runs`、`total_runs`、`pool`，以及 `runs[]` 中每个 seed 的 `seed/state/fe/total_fe/elapsed_seconds/error`。

Worker 运行时将 `progress.json` 内容 POST 到 `/api/v1/tasks/{task_id}/attempts/{attempt_id}/progress`；进程结束后调用 `/complete`。Master 的 WatchDog 以心跳与 Attempt 租约判断失联，超过三次心跳未到达则回收 Attempt，并将未完成 Seed 重新排队到其他 Worker。

## 调度边界

Master 回收的是整个批次 Task，而不是已完成的单个 seed。新的 Attempt 使用新的租约，旧 Worker 继续上报时会收到 `410`。因此 Worker 必须终止失效批次并保留本地文件，由管理员按需检查或导入；不能让两个 Worker 同时继续写入同一 Attempt。
