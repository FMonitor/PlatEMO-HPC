# 多 Worker Seed 调度

## 当前约定

当前运行模式是 Seed 级动态调度：所有算法-问题组合的 Seed 进入全局队列，哪个 Worker 有空闲并行池槽位，哪个 Worker 就在心跳中获得新的 Seed。

## 分配规则

- Master 不把问题绑定到某个 Worker；
- Worker 的可见性由环境约束、Profile、PlatEMO 版本、磁盘下限和节点暂停状态决定；
- `free_seed_slots` 是当前可提交的独立 Seed 数；
- 一个 Worker 可以同时运行不超过实际 `parpool` worker 数的 Seed；
- 一个 Seed 完成后立即释放 MATLAB 槽位，交付通过独立上传队列执行；
- 交付期间不占用计算槽位，但仍占用租约直到 complete 确认；
- `max_workers` 不再作为用户级任务绑定配置，调度容量以心跳中的实际空闲槽位为准。

## 心跳响应

Worker V2 心跳请求包含：

```json
{
  "session_id": "uuid",
  "pool": {"state": "ready", "configured_workers": 40, "actual_workers": 40},
  "free_seed_slots": 12,
  "running_seeds": []
}
```

响应包含 `assignments` 和 `cancel_attempt_ids`。每个 assignment 都有独立的 `attempt_id`、`lease_token`、Seed、算法/问题参数和环境约束。

## 验收条件

1. 40 槽 Worker 可以跨多个 ExperimentPoint 同时运行 40 个 Seed；
2. 任意单个 Seed 取消不会停止其他 Seed；
3. Worker 失联后仅未完成 Seed 回到 pending；
4. 产物上传失败会重试，不会提前确认 completed；
5. 进度、完成和产物均拒绝旧 lease token；
6. 多 Worker 不会重复领取同一个 Seed；
7. Worker 暂停接单只影响新分配，不影响已运行任务。
