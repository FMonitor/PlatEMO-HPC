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

1. Worker 拉取或接收一个租约任务。
2. 校验 lease token、PlatEMO commit、Settings MAT、磁盘空间与本地槽位。
3. 创建隔离工作目录：`Data/work/<task_id>/<attempt_no>/`。
4. 启动 `matlab.exe -batch` 调用统一的 `run_task.m`。
5. `run_task.m` 根据任务参数设置算法/问题，运行一个 seed 或由 Worker 打包的 seed 批次。
6. 定期写 `progress.json`；Worker 读取并上报。
7. 结束后校验 `result.mat`，上传结果、日志、哈希和退出码。
8. 成功/失败后释放本地槽位并领取下一任务。

## MATLAB 包装器约定

`run_task.m(taskJsonPath, workDir)` 必须：

- 不依赖 GUI，不调用 MATLAB Parallel Server。
- 读取算法名、算法参数、问题名、问题参数、seed、maxFE、Settings MAT 和保留数据点数。
- 可选启动本机 `parpool("Processes", localPoolSize)`；必须在结束时清理本任务创建的 pool。
- 至少每 `progress_interval_fe` 写入一次 JSON：`FE`、`totalFE`、`phase`、`timestamp`。
- 保留的过程数据不得超过任务设置的 `retain_points`。
- 在 `result.mat` 中记录任务快照、算法/问题参数、seed、MATLAB 版本、PlatEMO commit 和结束状态。

## 心跳和进度

Worker 每 10 秒上报一次；忙碌 Worker 的心跳还必须包含每个 Attempt 的：

- `task_id`、`attempt_id`、`lease_token`、PID。
- FE、total FE、进度、ETA 输入数据、运行时间。
- 最近日志尾部或日志 offset。
- 本地槽位占用、CPU、内存、GPU 利用率。

重启后 Worker 应扫描其工作目录，把存活 PID 重新上报；无法确认的旧工作目录不能直接标完成。

## 本地 WatchDog 和安全

- Worker 应杀死超过 Master 取消截止时间的 MATLAB 子进程，并上报 `cancelled`。
- 不执行 Master 任意传入的 shell 命令；只接受定义明确的任务 JSON。
- 只允许 Master 指定白名单下的 PlatEMO 文件、Settings MAT 和结果路径。
- API token/节点密钥只存放在 `config.json` 或受保护的系统凭据中，日志不得打印密钥。

