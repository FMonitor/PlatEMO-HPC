# Worker 批次与并行池约定

每个 Worker 是一个管理性质的计算节点。一个 Worker 同时运行一个批次 MATLAB 进程；批次由 Master 从同一算法-问题实验点的全局 Seed 队列切分。多个 Worker 可以并行处理同一实验点的不同 Seed。

```text
Worker -> 1 个 MATLAB 批次进程 -> 1 个 parpool(cluster_profile)
                               -> seed 1 ... seed N
```

- `seeds` 是 Master 签发给本 Worker 的切片，例如 30 次运行中的 Seed 1..10；它不是某个算法-问题的全部运行。
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

注册成功后，旧 Batch 模式每 10 秒向 `/api/v1/workers/{worker_id}/heartbeat` 上报。新安装默认使用 `execution_mode: "dynamic_seed_session"`：Worker 每 2 秒向 `/api/v2/workers/{worker_id}/heartbeat` 上报持久 MATLAB Supervisor 的实际 pool 大小、空闲 Seed 槽位与运行 Seed 摘要；Master 只为空闲槽位签发单个 Seed。不同实验点可共同填满同一个本机 `parpool`，Worker 不自行挑选任务。

## 动态会话部署

1. 关闭该节点的 `Start-Worker.ps1` 服务；不要用任务管理器结束其它人工启动的 MATLAB。
2. 覆盖 Worker 程序文件与 `platemo_worker/`，保留 `config.json`、`Data/` 和 `.venv/`。
3. 在 `config.json` 设置 `"execution_mode": "dynamic_seed_session"`。不需要设置 `max_seeds_per_batch`；动态模式以 MATLAB profile 实际创建的 pool worker 数为准。
4. 先启动 Master，再启动 `Start-Worker.ps1`。Worker 会先探测 profile，随后启动一个无头 MATLAB Supervisor 和一个持久 `parpool`。全局或节点“暂停接单”只阻止新 Seed，不会阻止 Supervisor 建池。
5. 首次发布先保持调度暂停，确认前端显示 `池 0/N` 到 `池 N/N` 的变化及 Worker 日志中的 `dynamic MATLAB session started`。恢复分配后用一个小规模实验验证 MAT 上传。

动态模式的 MATLAB 计算与 Python 上传队列分离。一个 Seed 完成后立即释放计算槽位，MAT 上传在后台串行重试并持续续租，不阻塞其它 Seed 补位。

## 任务确认

Worker 必须先上传 `result.mat` 或日志产物，再调用 BatchAttempt `complete`。失败和取消也必须调用 `complete`，即使没有 MAT。Master 返回 `410` 时停止继续上报该 BatchAttempt，并将本地产物保留在 `work/<experiment_point>/<batch_attempt>/` 等待重试或人工审查。

## Worker 实现要求

- Worker 启动后必须首先注册，再以 10 秒心跳维持在线状态；只能执行 Master 在心跳响应中签发的批次，不能自行选择任务或 Seed。
- 每个 Worker 同时只运行一个批次 MATLAB 进程。不要把多个批次塞进同一个 `parpool`，否则取消、FE 归属和失败回收无法可靠区分。
- 心跳响应中存在 `cancel_batch_attempt_ids` 时，Worker 必须终止对应 MATLAB 进程树，并以 `cancelled` 状态完成该 BatchAttempt。
- 仅在结果写入完成后上传 MAT；上传、进度和完成均携带同一 lease token。收到 `410` 后禁止再次上传或确认。
