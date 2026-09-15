# Master-Worker V2 接口协议

## 总则

V2 动态 Seed Session 是唯一运行协议。所有标识符使用 UUID，时间使用 ISO-8601 UTC。V1 Worker 接口统一返回 `410 protocol_v1_retired`。

## Worker 调用 Master

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| POST | `/api/v2/workers/register` | 使用 Join Token 注册并领取 Node Token |
| POST | `/api/v2/workers/{worker_id}/heartbeat` | 上报会话、并行池和 Seed，接收 assignment/取消 |
| POST | `/api/v2/seed-attempts/{attempt_id}/progress` | 上报单个 Seed 的 FE 和运行状态 |
| POST | `/api/v2/seed-attempts/{attempt_id}/reject` | 拒绝不兼容 assignment |
| POST | `/api/v2/seed-attempts/{attempt_id}/complete` | 确认 Seed 完成、失败或取消 |
| PUT | `/api/v2/artifacts/{artifact_id}` | 上传 Seed MAT |

## 心跳

```json
{
  "session_id": "uuid",
  "pool": {"state": "ready", "configured_workers": 40, "actual_workers": 40},
  "free_seed_slots": 12,
  "running_seeds": [
    {"attempt_id": "uuid", "experiment_point_id": "uuid", "seed": 8,
     "fe": 12000, "total_fe": 50000, "elapsed_seconds": 21}
  ]
}
```

响应包含 `assignments` 和 `cancel_attempt_ids`。Master 只在池容量为正、Worker 在线且未暂停时签发 assignment。

## Assignment 和租约

assignment 至少包含 `attempt_id`、`lease_token`、`experiment_point_id`、`seed`、算法、问题、`N/M/D/maxFE`、有序参数值和环境约束。Worker 校验成功后报告 accepted；不兼容时报告 rejected。所有 progress、artifact、complete 必须匹配 attempt 和 lease token，过期租约返回 410。

## 产物和完成

Worker 上传传统 MATLAB v7 MAT，变量为 `result` 和 `metric`。同一产物重试必须复用固定 artifact ID。Master 成功登记产物后才接受 `completed`；上传失败必须保留本地文件并重试。

## Master 提供给前端

主要查询接口包括：

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| POST | `/api/v1/experiments` | 创建实验和全局 SeedRun |
| GET | `/api/v1/ui/tasks` | 查询 Seed 状态、进度、耗时和 ETA |
| GET | `/api/v1/ui/workers` | 查询 Worker 状态和池容量 |
| POST | `/api/v1/seed-runs/history/retry` | 重新分配失败 Seed |
| POST | `/api/v1/seed-runs/history/delete` | 批量删除任务记录，不删除产物 |
| POST | `/api/v1/experiment-points/{id}/cancel` | 取消问题点全部未完成 Seed |

## 取消、失联和错误

用户取消将未完成 Seed 置为 `cancelled`。普通失联、进程失败和租约回收只将未完成 Seed 放回 `pending`。收到 410 后 Worker 停止对应 Seed 的后续写入，但其他 Seed 继续执行。连续三次心跳周期无响应时，Master 将 Worker 标记为 `suspect` 并回收未完成 Seed。
