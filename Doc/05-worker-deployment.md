# Worker 安装与部署

## 目录和配置

将 `worker/` 复制到 Windows 节点，并准备本机 PlatEMO 根目录。复制 `config.example.json` 为 `config.json`，设置：

```json
{
  "master_url": "http://<Master地址>:8000",
  "worker_id": "<Master登记的节点ID>",
  "worker_name": "节点名称",
  "platemo_root": "D:/PlatEMO",
  "matlab_exe": "matlab",
  "cluster_profile": "local"
}
```

并行池槽位由本机 MATLAB Profile 决定，Master 不在 assignment 中指定线程数。Worker 启动时异步探测 Profile；探测完成前上报零容量，不接收任务。

## 启动

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\Start-Worker.ps1
```

脚本会先输出 Worker 版本。Python 服务随后主动注册并开始 V2 心跳；无需手动打开 MATLAB。Profile 可用后，MATLAB Supervisor 自动启动并创建本地并行池。

## 本地目录

```text
Data/dynamic-session/
  inbox/       Master 下发的 Seed JSON
  outbox/      进度、完成和交付事件
  runs/        每个 Seed 的 MAT 和日志
  cancel/      Master 的取消标记
  logs/        MATLAB Supervisor 日志
  session.json 当前池摘要
```

## 更新和故障处理

更新前停止 Python Worker，再覆盖程序文件并重启。不要删除正在交付的 `Data/dynamic-session`，其中包含可重试产物和租约信息。Worker 会验证并管理自己的 MATLAB Supervisor；无法验证身份的遗留进程会被终止。
