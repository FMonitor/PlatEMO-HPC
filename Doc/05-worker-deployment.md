# Worker 批次与并行池约定

第一阶段只部署一个管理性质的 Worker 节点。一个 Worker 同时运行一个批次 MATLAB 进程；批次由一个算法、一个问题和多个随机种子组成。

```text
Worker -> 1 个 MATLAB 批次进程 -> 1 个 parpool(cluster_profile)
                               -> seed 1 ... seed N
```

- `seeds` 是任务级运行列表，例如一个算法-问题的 30 次运行。
- `cluster_profile` 使用 MATLAB 已配置的集群 profile；池大小由该 profile 决定。
- Worker 不再通过配置文件设置 MATLAB 任务并发数或池大小；每个 Worker 只维护一个批次进程。
- 批次内每个 seed 独立写入 `runs/seed-<seed>.mat`，并独立上报 FE/state。
- 停止粒度是整个批次：取消 Worker 的 MATLAB 进程和并行池；不提供 pool worker 或单个 seed 的强制停止。
- 已完成 seed 的结果必须保留，失败或未完成 seed 可由调度器在下一批次重新提交。

## 批次任务示例

```json
{
  "id": "b72b50e2-35d3-45aa-bd12-38a88ebf6c12",
  "algorithm": "VRLFSEA",
  "problem": "SMOP1",
  "seeds": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
  "N": 100,
  "M": 2,
  "D": 1000,
  "max_fe": 50000,
  "retain_points": 20,
  "cluster_profile": "local"
}
```

`cluster_profile` 必须是目标 MATLAB 已配置的 profile 名称。任务提交时不传则使用 `local`。

## v1 注册配置

Worker 配置必须包含：

- `master_url`：Master 的 ZeroTier 地址，例如 `http://10.x.x.x:6080`。
- `master_join_token`：Master 初始化后生成的 `worker_join_token`，只用于首次注册或重新注册。
- `worker_url`：本 Worker 的 ZeroTier 地址，例如 `http://10.x.x.x:6001`。
- `node_token`：首次注册成功后由 Worker 自动保存；不要手工复制到其他节点。
- `auto_run`：生产节点应为 `true`；设为 `false` 时节点只注册和心跳，不领取或执行任务，适合维护窗口。

注册成功后 Worker 每 10 秒向 `/api/v1/workers/{worker_id}/heartbeat` 上报，并在空闲时调用 `/api/v1/workers/{worker_id}/lease`。Master 不再向 Worker 主动 POST 任务。

## 任务确认

Worker 必须先上传 `result.mat` 或日志产物，再调用 Attempt `complete`。失败和取消也必须调用 `complete`，即使没有 MAT。Master 返回 `410` 时停止继续上报该 Attempt，并将本地产物保留在 `work/<task>/<attempt>/` 等待重试或人工审查。

## Worker 实现要求

- Worker 启动后必须首先注册，再以 10 秒心跳维持在线状态；不能由 Master 向 Worker 推送执行请求。
- 每个 Worker 同时只运行一个批次 MATLAB 进程。不要把多个批次塞进同一个 `parpool`，否则取消、FE 归属和失败回收无法可靠区分。
- 心跳响应中存在 `cancel_task_ids` 时，Worker 必须终止对应 MATLAB 进程树，并以 `cancelled` 状态完成该 Attempt。
- 仅在结果写入完成后上传 MAT；上传、进度和完成均携带同一 lease token。收到 `410` 后禁止再次上传或确认。
