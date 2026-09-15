# PlatEMO-HPC

PlatEMO-HPC 是一个面向 PlatEMO 的轻量级异构集群调度平台。Master 负责实验队列和结果登记，Worker 负责在各自机器上运行 MATLAB。当前使用 V2 动态 Seed 协议，不使用跨机器 MATLAB 并行池或 MATLAB Parallel Server。

## 工作方式

```text
浏览器 → Master(FastAPI + SQLite) ← HTTP/ZeroTier → Worker(Python)
                                                       └ MATLAB Supervisor
                                                          └ 本地 parpool
```

- 一次实验由多个算法实例、问题实例和 SeedRun 组成。
- 一个 SeedRun 是一次算法-问题-随机种子运行，也是调度、取消和重试的最小单位。
- 每个 Worker 只维护一个 MATLAB Supervisor 和一个本地并行池。
- 并行池的每个空闲槽位可运行一个独立 Seed；不同算法和问题可以混合填充。
- Seed 完成后立即释放计算槽位，结果在独立交付队列中上传，不阻塞其他 Seed。

## 目录

```text
master/       Master 后端、前端和启动脚本
worker/       Worker 程序、MATLAB 包装器和启动脚本
Doc/          中文设计、接口和部署文档
```

详细约定见 [Doc/README.md](Doc/README.md)。

## 一、启动 Master

环境要求：Windows、Python 3.11 或更高版本。进入 `master/` 执行：

```powershell
cd F:\Code\python\PlatEMO-HPC\master
.\Start-Master.ps1 -DataDir F:\PlatEMO-HPC-Data -PlatEMOPath H:\PlatEMO
```

首次启动脚本会创建 `.venv` 并安装 `requirements.txt`。默认监听 `0.0.0.0:6080`，浏览器打开：

```text
http://127.0.0.1:6080
```

Master 终端会输出 `Worker Join Token`。每台 Worker 首次注册都需要该 Token。`DataDir` 是 Master 的集中数据目录，包含 SQLite、Settings 上传文件和最终结果；生产运行时不要删除或移动它。

可选参数：

```powershell
.\Start-Master.ps1 -DataDir F:\PlatEMO-HPC-Data `
  -PlatEMOPath H:\PlatEMO `
  -HostAddress 0.0.0.0 -Port 6080 `
  -WorkerJoinToken '<固定的注册令牌>'
```

## 二、配置并启动 Worker

在每台计算节点上，将 `worker/` 复制到本机 PlatEMO 目录旁，例如 `H:\PlatEMO\worker`。首次启动：

```powershell
cd H:\PlatEMO\worker
.\Start-Worker.ps1
```

脚本会生成 `config.json` 并提示编辑。填写以下关键字段：

```json
{
  "platemo_root": "H:/PlatEMO",
  "matlab_exe": "H:/Matlab/bin/matlab.exe",
  "cluster_profile": "local",
  "worker_name": "Gold-6138-40",
  "worker_url": "http://<本机ZeroTier地址>:6001",
  "master_url": "http://<MasterZeroTier地址>:6080",
  "master_join_token": "<Master终端输出的Worker Join Token>",
  "auto_run": true
}
```

再次执行 `Start-Worker.ps1`：

```powershell
.\Start-Worker.ps1
```

Worker 启动时会先输出版本。它随后主动注册并发送 V2 心跳；MATLAB 不需要手动打开。Profile 探测完成后，Worker 自动启动一个无头 MATLAB Supervisor 和本地 `parpool`。并行池槽位数量由 MATLAB Profile 和本机配置决定，Master 不直接控制线程数。

`data_dir` 默认是 Worker 目录下的 `Data`，只保存本地队列、进度、日志和待上传结果，不是 Master 数据目录。更新 Worker 时先停止 Python 服务，再覆盖代码并重启；不要删除仍在交付中的 `Data/dynamic-session`。

## 三、前端使用

1. 在 Master 前端刷新 PlatEMO 目录。
2. 导入 `Data/Setting*.mat`，或在编辑器中选择算法、问题并设置参数。
3. 设置每个问题实例的 `N`、`M`、`D`、`maxFE`、运行次数和保留数据点数。
4. 选择可工作的 Worker，点击“启动任务”。任务进入全局待分配队列。
5. Master 根据 Worker 心跳中的空闲池槽位动态分配 Seed，不需要手动指定某个问题绑定到某个 Worker。
6. 在“计划分配”中查看待分配任务，在“任务记录”中查看运行、完成、失败和取消的 Seed。
7. 可暂停某个 Worker 的接单；暂停只阻止新 Seed，不停止该节点已经运行或交付中的 Seed。
8. 失败任务可多选后重新分配；取消任务不会再次入队。

## 四、结果位置和格式

每个成功 Seed 上传一个传统 PlatEMO MATLAB v7 文件，包含 `result` 和 `metric` 变量。Master 按实验和算法分类保存：

```text
<Master DataDir>/experiments/<experiment_id>/<algorithm>/
  <algorithm>_<problem>_M<M>_D<D>_<seed>.mat
```

文件可以复制到 PlatEMO 的 `Data/<algorithm>/` 下供后续读取。任务状态、租约、错误、进度和审计信息保存在 Master SQLite 及 Worker 本地 JSON 中，不写入 MAT 文件。

## 五、状态和故障排查

- `等待分配`：Worker 在线且有空闲槽位，等待 Master 签发 Seed。
- `运行中`：Seed 已提交给 MATLAB Future，正在产生 FE 进度。
- `交付中`：MATLAB 已结束，Worker 正在上传结果或重试 complete；此时不占用计算槽位。
- `不可接单`：Profile 未就绪、容量为零、节点暂停、环境不兼容或心跳失联。
- `离线/可疑`：Master 超过连续三个心跳周期未收到有效心跳，会回收未完成 Seed。

常用日志位置：

```text
<Worker>/Data/logs/worker.log
<Worker>/Data/dynamic-session/logs/matlab-supervisor.log
<Master DataDir>/master.sqlite3
```

如果 MATLAB 并行池因空闲超时或异常关闭，Supervisor 会检测并自动重建，尚未提交的 inbox 任务会保留并重试。若任务收到 HTTP 410，表示租约已失效，Worker 会停止该 Seed 的后续写入，Master 可重新分配未完成 Seed。

## 六、更新前端

修改 Vue 前端后，在 `master/` 执行：

```powershell
.\Build-Frontend.ps1
```

构建产物写入 `master/static/`，重启 Master 后生效。

## 协议说明

当前 Worker 只使用 `/api/v2` 注册、心跳、Seed 进度、取消、产物和完成接口。旧 `/api/v1` Worker 调度接口已经退役，调用会返回 `410 protocol_v1_retired`。
