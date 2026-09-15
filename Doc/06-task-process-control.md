# Seed 进程控制与取消

## 控制边界

一个 Seed 对应一个 MATLAB `parfeval` Future。取消只针对该 Future，不关闭整个 MATLAB Supervisor，也不影响同池其他 Seed。

## 取消流程

1. 用户调用 `/api/v2/seed-attempts/{attempt_id}/cancel`。
2. Master 在下一次 Worker 心跳响应中返回 `cancel_attempt_ids`。
3. Worker 写入对应的本地取消标记。
4. MATLAB Supervisor 只取消对应 Future，并写入 `cancelled` 终态事件。
5. Master 将该 Seed 标记为 cancelled，不再重新入队。

如果任务已经进入交付阶段，Worker 不再中断上传；Master 根据取消竞态决定最终是否保留已完成结果。

## 租约失效

进度、产物或完成请求收到 410 后，Worker 必须停止该 Seed 的后续写入。动态 Supervisor 仍可继续运行其他 Future；失效 Seed 的本地 MAT、JSON 和日志保留以便审计。

## 重启安全

Worker 持久化 Supervisor PID、启动表达式指纹和会话目录。重启时只对能验证为本 Worker 的 MATLAB 进程继续管理；无法验证的进程树必须终止。Worker 不会仅凭旧 inbox JSON 重新启动已经失去租约的 Seed。
