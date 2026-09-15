# Master 与前端职责

## Master 后端

Master 位于 `master/`，入口为 `app.py`，使用 FastAPI 和 SQLite。它负责扫描 PlatEMO 目录、创建实验快照、生成全局 SeedRun、按 Worker 心跳分配 Seed、校验租约、接收进度和结果，并运行 WatchDog 回收失联任务。

## 实验快照

一次提交生成一个不可变 Experiment。每个算法实例和问题实例组成一个 ExperimentPoint，每个点按运行次数生成 SeedRun。`N/M/D/maxFE` 是 assignment 顶层字段；算法和问题自定义参数分别使用有序 `algorithm_parameter_values`、`problem_parameter_values`。

## 调度

Master 不将问题绑定到某个 Worker。只有满足环境约束、Profile、版本、磁盘和暂停状态的 Worker 才会在 V2 心跳中收到 assignment。每个 Seed 使用唯一 `attempt_id + lease_token`，accepted、progress、artifact、complete 均校验租约。

## WatchDog

心跳和进度会续租。租约过期返回 410，Master 只回收未完成 Seed；已登记产物的 Seed 不重跑。普通失联或运行失败回到 pending，用户取消进入 cancelled，不再重试。

## 前端

前端显示计划分配、任务记录、Worker 状态、实际池容量、Seed FE/MaxFE/耗时/ETA、错误和结果。任务记录支持分页、全页勾选、批量删除和重新分配失败任务。Worker 的暂停按钮只暂停新接单，不停止已运行 Seed。

## 数据目录

结果由 V2 artifact 接口原子写入 `Data/experiments/<experiment_id>/<algorithm>/`，文件名为 `<algorithm>_<problem>_M<M>_D<D>_<seed>.mat`。
