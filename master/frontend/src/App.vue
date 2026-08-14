<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { ChevronDown, ChevronUp, CircleStop, Cog, FileUp, FolderCog, Pause, Pencil, Play, Plus, RefreshCw, Save, Search, Server, Trash2, X } from '@lucide/vue'
import { api } from './api'
import type { CatalogItem, ExistingTest, ExperimentItem, ImportedSettings, TaskStatus, Worker } from './types'

const algorithms = ref<CatalogItem[]>([])
const problems = ref<CatalogItem[]>([])
const existingTests = ref<ExistingTest[]>([])
const selectedExistingTestKeys = ref<string[]>([])
const taskStatuses = ref<TaskStatus[]>([])
const selectedHistoryKeys = ref<string[]>([])
const scheduleView = ref<'queue' | 'history'>('queue')
const queueExpanded = ref(true)
const historyExpanded = ref(true)
const queuePage = ref(1)
const queuePageSize = ref(20)
const historyPage = ref(1)
const historyPageSize = ref(20)
const workers = ref<Worker[]>([])
const selectedAlgorithms = ref<ExperimentItem[]>([])
const selectedProblems = ref<ExperimentItem[]>([])
const algorithmSearch = ref('')
const problemSearch = ref('')
const platemoPath = ref('')
const runs = ref(30)
const retainPoints = ref(20)
const clusterProfile = ref('')
const requiredPlatemoCommit = ref('')
const minimumDiskFreeBytes = ref<number | null>(null)
const experimentSettingsFile = ref<File | null>(null)
const showWorkerForm = ref(false)
const showPlatemoForm = ref(false)
const editingWorker = ref<string | null>(null)
const workerForm = ref({ name: '', url: '', token: '', priority: 0 })
const busy = ref(false)
const workersRefreshing = ref(false)
const workerPauseChanging = ref<string | null>(null)
const initialLoading = ref(true)
const dispatchPaused = ref(false)
type Toast = { id: number; message: string; kind: 'notice' | 'error' }
const toasts = ref<Toast[]>([])
const error = ref('')
const importedDiagnostics = ref<string[]>([])
const uploadInput = ref<HTMLInputElement | null>(null)
const workspace = ref<HTMLElement | null>(null)
const columnWidths = ref({ catalog: 300, editor: 220, existing: 330 })
const catalogHeights = ref({ algorithms: 270, problems: 270 })
let noticeId = 0
const toastTimers = new Map<number, ReturnType<typeof window.setTimeout>>()
let taskRefreshTimer: ReturnType<typeof window.setInterval> | undefined

type ColumnResizeTarget = 'catalog' | 'editor' | 'existing'
type CatalogResizeTarget = 'algorithms' | 'problems'

type ResizeState =
  | { direction: 'column'; target: ColumnResizeTarget; origin: number; widths: { catalog: number; editor: number; existing: number } }
  | { direction: 'row'; target: CatalogResizeTarget; origin: number; heights: { algorithms: number; problems: number } }

const resizeState = ref<ResizeState | null>(null)

const workspaceStyle = computed(() => ({
  '--catalog-width': `${columnWidths.value.catalog}px`,
  '--editor-width': `${columnWidths.value.editor}px`,
  '--existing-width': `${columnWidths.value.existing}px`,
}))

const catalogStyle = computed(() => ({
  '--algorithm-height': `${catalogHeights.value.algorithms}px`,
  '--problem-height': `${catalogHeights.value.problems}px`,
}))

const filteredAlgorithms = computed(() => filterCatalog(algorithms.value, algorithmSearch.value))
const filteredProblems = computed(() => filterCatalog(problems.value, problemSearch.value))
const totalTasks = computed(() => selectedAlgorithms.value.length * selectedProblems.value.length * runs.value)
const availableWorkerCount = computed(() => workers.value.filter((worker) => worker.online && !Number(worker.dispatch_paused ?? 0)).length)
const activeTaskCount = computed(() => taskStatuses.value.filter((task) => ['queued', 'leased', 'accepted', 'running', 'cancel_requested'].includes(task.state)).length)
const pendingTasks = computed(() => taskStatuses.value.filter((task) => task.state === 'pending'))
const queuePageCount = computed(() => Math.max(1, Math.ceil(pendingTasks.value.length / queuePageSize.value)))
const pagedPendingTasks = computed(() => {
  const start = (queuePage.value - 1) * queuePageSize.value
  return pendingTasks.value.slice(start, start + queuePageSize.value)
})
const claimedTaskCount = computed(() => taskStatuses.value.filter((task) => ['queued', 'leased', 'accepted'].includes(task.state)).length)
const runningTaskCount = computed(() => taskStatuses.value.filter((task) => ['running', 'cancel_requested'].includes(task.state)).length)
const seedRunTasks = computed(() => taskStatuses.value.filter((task) => ['queued', 'leased', 'accepted', 'running', 'cancel_requested'].includes(task.state)))
const taskHistory = computed(() => taskStatuses.value
  .filter((task) => ['completed', 'failed'].includes(task.state))
  .sort((left, right) => String(right.updated_at).localeCompare(String(left.updated_at))))
const historyPageCount = computed(() => Math.max(1, Math.ceil(taskHistory.value.length / historyPageSize.value)))
const pagedTaskHistory = computed(() => {
  const start = (historyPage.value - 1) * historyPageSize.value
  return taskHistory.value.slice(start, start + historyPageSize.value)
})
const currentPageHistoryKeys = computed(() => pagedTaskHistory.value.map(historyTaskKey))
const currentPageHistorySelected = computed(() => currentPageHistoryKeys.value.length > 0
  && currentPageHistoryKeys.value.every((key) => selectedHistoryKeys.value.includes(key)))

function historyTaskKey(task: TaskStatus) {
  return `${task.task_id || task.id.split(':')[0]}:${task.seed}`
}

function setCurrentHistoryPageSelected(selected: boolean) {
  const current = new Set(currentPageHistoryKeys.value)
  if (selected) {
    selectedHistoryKeys.value = [...new Set([...selectedHistoryKeys.value, ...current])]
  } else {
    selectedHistoryKeys.value = selectedHistoryKeys.value.filter((key) => !current.has(key))
  }
}

function toggleCurrentHistoryPage(event: Event) {
  setCurrentHistoryPageSelected((event.target as HTMLInputElement).checked)
}

function existingTestKey(test: ExistingTest) {
  return `${test.algorithm}:${test.problem}:M${test.M}:D${test.D}`
}

function filterCatalog(items: CatalogItem[], query: string) {
  const normalized = query.trim().toLowerCase()
  return normalized ? items.filter((item) => item.name.toLowerCase().includes(normalized)) : items
}

function clamp(value: number, minimum: number, maximum: number) {
  return Math.min(Math.max(value, minimum), Math.max(minimum, maximum))
}

function showNotice(message: string) {
  showToast(message, 'notice')
}

function showToast(message: string, kind: Toast['kind']) {
  const id = ++noticeId
  toasts.value.push({ id, message, kind })
  toastTimers.set(id, window.setTimeout(() => {
    toasts.value = toasts.value.filter((toast) => toast.id !== id)
    toastTimers.delete(id)
  }, 4600))
}

watch(error, (message) => {
  if (!message) return
  showToast(message, 'error')
  error.value = ''
})

watch([taskHistory, historyPageSize], () => {
  historyPage.value = Math.min(historyPage.value, historyPageCount.value)
})

watch([pendingTasks, queuePageSize], () => {
  queuePage.value = Math.min(queuePage.value, queuePageCount.value)
})

function stopResize() {
  resizeState.value = null
  document.body.classList.remove('is-resizing')
  window.removeEventListener('pointermove', resize)
  window.removeEventListener('pointerup', stopResize)
}

function resize(event: PointerEvent) {
  const state = resizeState.value
  if (!state) return

  if (state.direction === 'column') {
    const width = workspace.value?.clientWidth ?? 1000
    const minimumTaskWidth = 480
    const handleWidth = 24
    const delta = event.clientX - state.origin

    if (state.target === 'catalog') {
      columnWidths.value.catalog = clamp(state.widths.catalog + delta, 300, width - state.widths.editor - state.widths.existing - minimumTaskWidth - handleWidth)
    } else if (state.target === 'editor') {
      columnWidths.value.editor = clamp(state.widths.editor + delta, 220, width - state.widths.catalog - state.widths.existing - minimumTaskWidth - handleWidth)
    } else {
      columnWidths.value.existing = clamp(state.widths.existing + delta, 300, width - state.widths.catalog - state.widths.editor - minimumTaskWidth - handleWidth)
    }
    return
  }

  const delta = event.clientY - state.origin
  const minimumAlgorithmHeight = 120
  const minimumProblemHeight = 120
  if (state.target === 'algorithms') {
    catalogHeights.value = { ...state.heights, algorithms: Math.max(minimumAlgorithmHeight, state.heights.algorithms + delta) }
    return
  }

  if (state.target === 'problems') {
    catalogHeights.value = { ...state.heights, problems: Math.max(minimumProblemHeight, state.heights.problems + delta) }
    return
  }
}

function startColumnResize(target: ColumnResizeTarget, event: PointerEvent) {
  event.preventDefault()
  stopResize()
  resizeState.value = { direction: 'column', target, origin: event.clientX, widths: { ...columnWidths.value } }
  document.body.classList.add('is-resizing')
  window.addEventListener('pointermove', resize)
  window.addEventListener('pointerup', stopResize, { once: true })
}

function startCatalogResize(target: CatalogResizeTarget, event: PointerEvent) {
  event.preventDefault()
  stopResize()
  resizeState.value = { direction: 'row', target, origin: event.clientY, heights: { ...catalogHeights.value } }
  document.body.classList.add('is-resizing')
  window.addEventListener('pointermove', resize)
  window.addEventListener('pointerup', stopResize, { once: true })
}

function makeItem(item: CatalogItem, prefix: string): ExperimentItem {
  return {
    id: `${prefix}-${item.name}-${crypto.randomUUID()}`,
    name: item.name,
    parameters: Object.fromEntries(item.parameters.map((parameter) => [parameter.name, parameter.default])),
  }
}

function addAlgorithm(item: CatalogItem) {
  if (!selectedAlgorithms.value.some((chosen) => chosen.name === item.name)) {
    selectedAlgorithms.value.push(makeItem(item, 'algorithm'))
  }
}

function addProblem(item: CatalogItem) {
  selectedProblems.value.push(makeItem(item, 'problem'))
}

function removeItem(items: ExperimentItem[], id: string) {
  const index = items.findIndex((item) => item.id === id)
  if (index >= 0) items.splice(index, 1)
}

function schema(item: ExperimentItem, type: 'algorithm' | 'problem') {
  const source = (type === 'algorithm' ? algorithms.value : problems.value).find((candidate) => candidate.name === item.name)
  return source?.parameters ?? []
}

function applyImported(values: ImportedSettings) {
  selectedAlgorithms.value = values.algorithms.map((item) => ({ ...item, id: `algorithm-${crypto.randomUUID()}` }))
  selectedProblems.value = values.problems.map((item) => ({ ...item, id: `problem-${crypto.randomUUID()}` }))
  runs.value = Number(values.runs) || 30
  retainPoints.value = Number(values.retain_results) || 20
  importedDiagnostics.value = values.diagnostics
  showNotice(`已加载 ${values.source}：${values.algorithms.length} 个算法、${values.problems.length} 个问题实例。`)
}

async function loadCatalog() {
  const catalog = await api.catalog()
  algorithms.value = catalog.algorithms
  problems.value = catalog.problems
  existingTests.value = catalog.existing_tests
  platemoPath.value = catalog.platemo_path
}

async function loadTasks() {
  taskStatuses.value = await api.tasks()
  const retained = new Set(taskStatuses.value
    .filter((task) => ['completed', 'failed'].includes(task.state))
    .map(historyTaskKey))
  selectedHistoryKeys.value = selectedHistoryKeys.value.filter((key) => retained.has(key))
}

async function refreshWorkers() {
  workersRefreshing.value = true
  error.value = ''
  try {
    await api.probeWorkers()
    workers.value = await api.workers()
    showNotice(`已刷新 ${workers.value.length} 个 Worker 节点。`)
  } catch (caught) {
    error.value = caught instanceof Error ? caught.message : '刷新 Worker 失败'
  } finally {
    workersRefreshing.value = false
  }
}

async function refreshAll() {
  busy.value = true
  error.value = ''
  try {
    await loadCatalog()
    workers.value = await api.workers()
    dispatchPaused.value = (await api.schedulerStatus()).paused
    await loadTasks()
  } catch (caught) {
    error.value = caught instanceof Error ? caught.message : '读取 Master 数据失败'
  } finally {
    busy.value = false
  }
}

async function refreshDashboard() {
  try {
    workers.value = await api.workers()
    dispatchPaused.value = (await api.schedulerStatus()).paused
    await loadTasks()
  } catch {
    // Preserve the current view during transient background refresh failures.
  }
}

async function savePlatemoPath() {
  busy.value = true
  error.value = ''
  try {
    const response = await api.setPlatemoPath(platemoPath.value)
    platemoPath.value = response.platemo_path
    showPlatemoForm.value = false
    await loadCatalog()
    showNotice('PlatEMO 目录已解析。')
  } catch (caught) {
    error.value = caught instanceof Error ? caught.message : '设置 PlatEMO 路径失败'
  } finally {
    busy.value = false
  }
}

async function importFile(event: Event) {
  const file = (event.target as HTMLInputElement).files?.[0]
  if (!file) return
  busy.value = true
  error.value = ''
  try {
    const response = await api.importSetting(file)
    applyImported(response.values)
    experimentSettingsFile.value = file
  } catch (caught) {
    error.value = caught instanceof Error ? caught.message : '导入设置失败'
  } finally {
    busy.value = false
    if (uploadInput.value) uploadInput.value.value = ''
  }
}

function addExistingTest(test: ExistingTest) {
  const problem = problems.value.find((item) => item.name === test.problem)
  if (!problem) {
    error.value = `现有结果中的问题 ${test.problem} 不在当前 PlatEMO 目录中`
    return
  }
  const instance = makeItem(problem, 'problem')
  instance.parameters.M = test.M
  instance.parameters.D = test.D
  selectedProblems.value.push(instance)
}

async function saveNativeSetting() {
  const filename = window.prompt('另存为文件名', 'platemo-hpc-setting.mat')
  if (!filename) return
  try {
    const blob = await api.saveSetting({
      format: 'platemo-hpc-settings',
      version: 1,
      algorithms: selectedAlgorithms.value,
      problems: selectedProblems.value,
      execution: { runs: runs.value, retain_points: retainPoints.value },
      environment: { cluster_profile: clusterProfile.value, required_platemo_commit: requiredPlatemoCommit.value, minimum_disk_free_bytes: minimumDiskFreeBytes.value },
    }, filename)
    const url = URL.createObjectURL(blob)
    const anchor = document.createElement('a')
    anchor.href = url
    anchor.download = filename.toLowerCase().endsWith('.mat') ? filename : `${filename}.mat`
    anchor.click()
    URL.revokeObjectURL(url)
    showNotice('已导出 Master 原生设置文件。')
  } catch (caught) {
    error.value = caught instanceof Error ? caught.message : '保存设置失败'
  }
}

function openAddWorker() {
  workerForm.value = { name: '', url: '', token: '', priority: 0 }
  editingWorker.value = null
  showWorkerForm.value = true
}

function openEditWorker(worker: Worker) {
  workerForm.value = { name: worker.name, url: worker.url, token: '', priority: worker.priority ?? 0 }
  editingWorker.value = worker.id
  showWorkerForm.value = true
}

async function saveWorker() {
  if (!workerForm.value.name || !workerForm.value.url) return
  busy.value = true
  error.value = ''
  const editingId = editingWorker.value
  const form = { ...workerForm.value }
  const previousWorkers = workers.value
  try {
    if (editingId) {
      workers.value = workers.value.map((worker) => worker.id === editingId
        ? { ...worker, name: form.name, url: form.url, priority: form.priority }
        : worker)
      showWorkerForm.value = false
      await api.editWorker(editingId, form)
      showNotice(`已更新节点 ${form.name} 属性。`)
      return
    }
    await api.addWorker(form)
    showWorkerForm.value = false
    workers.value = await api.workers()
    showNotice(`已添加节点 ${form.name}。`)
  } catch (caught) {
    if (editingId) {
      workers.value = previousWorkers
      showWorkerForm.value = true
    }
    error.value = caught instanceof Error ? caught.message : '保存 Worker 失败'
  } finally {
    busy.value = false
  }
}

async function deleteWorker(worker: Worker) {
  if (!window.confirm(`删除 Worker “${worker.name}”？`)) return
  const previousWorkers = workers.value
  workers.value = workers.value.filter((item) => item.id !== worker.id)
  busy.value = true
  error.value = ''
  try {
    await api.deleteWorker(worker.id)
    showNotice(`已删除节点 ${worker.name}。`)
  } catch (caught) {
    workers.value = previousWorkers
    error.value = caught instanceof Error ? caught.message : '删除 Worker 失败'
  } finally {
    busy.value = false
  }
}

async function startRun() {
  error.value = ''
  if (!selectedAlgorithms.value.length || !selectedProblems.value.length) {
    error.value = '需要至少选择一个算法和一个问题实例。'
    return
  }
  busy.value = true
  try {
    await api.startRun({
      algorithms: selectedAlgorithms.value,
      problems: selectedProblems.value,
      runs: runs.value,
      retainPoints: retainPoints.value,
      clusterProfile: clusterProfile.value,
      requiredPlatemoCommit: requiredPlatemoCommit.value,
      minimumDiskFreeBytes: minimumDiskFreeBytes.value ?? undefined,
      settingsUpload: experimentSettingsFile.value,
    })
    await loadTasks()
    showNotice(`已创建 ${totalTasks.value} 个任务。`)
  } catch (caught) {
    error.value = caught instanceof Error ? caught.message : '创建任务失败'
  } finally {
    busy.value = false
  }
}

function taskParameter(task: TaskStatus, name: string) {
  const value = task.parameters[name]
  return value === undefined || value === '' ? '—' : String(value)
}

function taskStateLabel(task: TaskStatus) {
  if (task.state === 'queued') return '已入队'
  if (task.state === 'leased') return '已领取'
  if (task.state === 'cancel_requested') return '正在取消'
  if (task.state === 'running') return '运行中'
  return task.state
}

function workerPoolUsage(worker: Worker) {
  const session = worker.dynamic_session
  if (session) return `${session.running_seeds}/${session.actual_workers || session.configured_workers || '--'}`
  const total = Number(worker.configured_pool_workers ?? 0)
  if (!Number.isFinite(total) || total < 1) return '--/--'
  const pool = worker.active_batch?.pool ?? {}
  const actual = Number(pool.actual_pool_workers ?? (pool.pool as Record<string, unknown> | undefined)?.workers ?? 0)
  return actual > 0 ? `${actual}/${total}` : `0/${total}`
}

function workerStatus(worker: Worker) {
  if (!worker.online || worker.status === 'suspect') return { label: worker.status === 'suspect' ? '心跳失联' : '离线', tone: 'offline', detail: '等待 Worker 恢复心跳' }
  if (Number(worker.dispatch_paused ?? 0) > 0) return { label: '暂停接单', tone: 'paused', detail: '' }
  const batch = worker.active_batch
  const session = worker.dynamic_session
  if (session && session.actual_workers > 0) {
    return session.running_seeds > 0
      ? { label: '运行中', tone: 'running', detail: '' }
      : { label: '等待分配', tone: 'idle', detail: '' }
  }
  if (batch?.state === 'cancel_requested') return { label: '取消中', tone: 'cancelling', detail: '正在停止当前批次' }
  if (batch?.state === 'assigned') return { label: '等待确认', tone: 'assigned', detail: `等待确认 ${assignmentCountdown(batch.lease_deadline)}` }
  if (batch?.state === 'accepted') return { label: '启动中', tone: 'starting', detail: '正在启动 MATLAB 和并行池' }
  if (batch?.state === 'running') {
    const pool = batch.pool ?? {}
    const phase = String((pool.pool as Record<string, unknown> | undefined)?.phase ?? '')
    if (phase === 'delivering') return { label: '交付中', tone: 'delivering', detail: '正在上传结果' }
    return { label: '运行中', tone: 'running', detail: '' }
  }
  const fallback = taskStatuses.value.find((task) => task.worker_id === worker.id && ['queued', 'leased', 'accepted', 'running', 'cancel_requested'].includes(task.state))
  if (fallback?.state === 'cancel_requested') return { label: '取消中', tone: 'cancelling', detail: '正在停止当前批次' }
  if (fallback?.state === 'leased') return { label: '等待确认', tone: 'assigned', detail: '' }
  if (fallback?.state === 'accepted') return { label: '启动中', tone: 'starting', detail: '正在启动 MATLAB 和并行池' }
  if (fallback?.state === 'queued') return { label: '启动中', tone: 'starting', detail: '正在启动 MATLAB 和并行池' }
  if (fallback?.state === 'running') return { label: '运行中', tone: 'running', detail: '' }
  if (Number(worker.dispatch_paused ?? 0) > 0) return { label: '暂停接单', tone: 'paused', detail: '' }
  if (!worker.profile_ready) return { label: '初始化中', tone: 'starting', detail: '正在检测 MATLAB 和并行池' }
  if (Number(worker.queue_count ?? 0) < 1) return { label: '不可接单', tone: 'paused', detail: '没有可用批次槽位' }
  return { label: '等待分配', tone: 'idle', detail: '' }
}

function assignmentCountdown(deadline: string) {
  const remaining = Math.max(0, Math.ceil((new Date(deadline).getTime() - Date.now()) / 1000))
  return `${remaining}s`
}

function heartbeatAge(worker: Worker) {
  if (!worker.last_heartbeat) return '--'
  const seconds = Math.max(0, Math.floor((Date.now() - new Date(worker.last_heartbeat).getTime()) / 1000))
  return seconds < 60 ? `${seconds}s` : `${Math.floor(seconds / 60)}m`
}

function isCancellableTask(task: TaskStatus) {
  return Boolean(task.batch_attempt_id) && ['queued', 'leased', 'accepted', 'running', 'cancel_requested'].includes(task.state)
}

async function cancelTaskBatch(task: TaskStatus) {
  if (!task.batch_attempt_id || !isCancellableTask(task)) return
  if (!window.confirm(`取消 ${task.worker_name} 上的批次？该批次内的 Seed 将一并停止。`)) return
  busy.value = true
  error.value = ''
  try {
    if (task.attempt_kind === 'dynamic_seed') await api.cancelSeedAttempt(task.batch_attempt_id)
    else await api.cancelBatch(task.batch_attempt_id)
    await loadTasks()
    showNotice('已请求取消 Worker 批次。')
  } catch (caught) {
    error.value = caught instanceof Error ? caught.message : '请求取消批次失败'
  } finally {
    busy.value = false
  }
}

function formatParameter(value: unknown) {
  if (Array.isArray(value)) return value.join(',')
  if (value && typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

function parameterSummary(task: TaskStatus) {
  const problem = Object.entries(task.parameters ?? {}).map(([key, value]) => `${key}=${formatParameter(value)}`)
  const algorithm = Object.entries(task.algorithm_parameters ?? {}).map(([key, value]) => `${key}=${formatParameter(value)}`)
  return `${[task.problem, ...problem].join(' ')} | ${[task.algorithm, ...algorithm].join(' ')}`
}

function inlineTaskSummary(task: TaskStatus) {
  const problem = Object.entries(task.parameters ?? {}).map(([key, value]) => `${key}=${formatParameter(value)}`)
  const algorithm = Object.entries(task.algorithm_parameters ?? {}).map(([key, value]) => `${key}=${formatParameter(value)}`)
  const problemText = [task.problem, ...problem].join(' ')
  const algorithmText = [task.algorithm, ...algorithm].join(' ')
  return `${problemText} | ${algorithmText}`
}

async function toggleDispatchPause(paused = !dispatchPaused.value) {
  busy.value = true
  try {
    const result = await api.setSchedulerPaused(paused)
    dispatchPaused.value = result.paused
    showNotice(result.paused ? '已暂停新任务分配。' : '已恢复任务分配。')
  } catch (caught) {
    error.value = caught instanceof Error ? caught.message : '更新分配状态失败'
  } finally {
    busy.value = false
  }
}

async function toggleWorkerDispatchPause(worker: Worker) {
  workerPauseChanging.value = worker.id
  error.value = ''
  try {
    const paused = !Boolean(Number(worker.dispatch_paused ?? 0))
    await api.setWorkerDispatchPaused(worker.id, paused)
    workers.value = await api.workers()
    showNotice(paused ? `${worker.name} 已暂停接单。` : `${worker.name} 已恢复接单。`)
  } catch (caught) {
    error.value = caught instanceof Error ? caught.message : '更新 Worker 接单状态失败'
  } finally {
    workerPauseChanging.value = null
  }
}

async function cancelPendingTask(task: TaskStatus) {
  const pointId = task.task_id || task.id.split(':')[0]
  if (!pointId || !window.confirm(`取消问题 ${task.problem} 的待分配任务？`)) return
  busy.value = true
  try {
    await api.cancelPoint(pointId)
    await loadTasks()
    showNotice('已取消该问题实例的待分配任务。')
  } catch (caught) {
    error.value = caught instanceof Error ? caught.message : '取消待分配任务失败'
  } finally {
    busy.value = false
  }
}

async function cancelAllTasks() {
  if (!window.confirm('取消所有待分配和运行中的任务？')) return
  busy.value = true
  try {
    await api.cancelAllTasks()
    await loadTasks()
    showNotice('已请求取消所有任务。')
  } catch (caught) {
    error.value = caught instanceof Error ? caught.message : '取消所有任务失败'
  } finally {
    busy.value = false
  }
}

async function deleteTaskHistory(task: TaskStatus) {
  const pointId = task.task_id || task.id.split(':')[0]
  if (!pointId || !window.confirm(`删除 ${task.problem} Seed ${task.seed} 的任务记录？`)) return
  busy.value = true
  try {
    await api.deleteSeedHistory(pointId, task.seed)
    await loadTasks()
    showNotice('任务记录已删除。')
  } catch (caught) {
    error.value = caught instanceof Error ? caught.message : '删除任务记录失败'
  } finally {
    busy.value = false
  }
}

async function deleteSelectedTaskHistories() {
  const selected = taskHistory.value.filter((task) => selectedHistoryKeys.value.includes(historyTaskKey(task)))
  if (!selected.length) return
  if (!window.confirm(`删除所选 ${selected.length} 条任务记录？已上传的 MAT 结果文件不会删除。`)) return
  const runs = selected.map((task) => ({ pointId: task.task_id || task.id.split(':')[0], seed: task.seed }))
  if (runs.some((run) => !run.pointId)) return
  busy.value = true
  try {
    const result = await api.deleteSeedHistories(runs)
    selectedHistoryKeys.value = []
    await loadTasks()
    showNotice(`已删除 ${result.count} 条任务记录。`)
  } catch (caught) {
    error.value = caught instanceof Error ? caught.message : '批量删除任务记录失败'
  } finally {
    busy.value = false
  }
}

function taskProgress(task: TaskStatus) {
  const fe = Number(task.fe)
  const total = Number(task.total_fe)
  if (!Number.isFinite(fe) || !Number.isFinite(total) || total <= 0) return 0
  return Math.round(Math.min(Math.max((fe / total) * 100, 0), 100))
}

function taskFeLabel(task: TaskStatus) {
  const fe = Number(task.fe)
  const total = Number(task.total_fe)
  if (!Number.isFinite(fe) || !Number.isFinite(total) || total <= 0) return 'FE 等待上报'
  return `FE ${fe.toLocaleString()} / ${total.toLocaleString()}`
}

function formatDuration(seconds: number | undefined) {
  if (!Number.isFinite(seconds) || !seconds || seconds < 0) return '—'
  const value = Math.floor(seconds)
  const hours = Math.floor(value / 3600)
  const minutes = Math.floor((value % 3600) / 60)
  const remainingSeconds = value % 60
  if (hours > 0) return `${hours}h ${String(minutes).padStart(2, '0')}m`
  if (minutes > 0) return `${minutes}m ${String(remainingSeconds).padStart(2, '0')}s`
  return `${remainingSeconds}s`
}

function taskEtaLabel(task: TaskStatus) {
  if (task.state === 'completed') return '已完成'
  const fe = Number(task.fe)
  const total = Number(task.total_fe)
  const elapsed = Number(task.elapsed_seconds)
  if (!Number.isFinite(fe) || !Number.isFinite(total) || !Number.isFinite(elapsed) || fe <= 0 || total <= fe || elapsed <= 0) return 'ETA 等待上报'
  return `ETA ${formatDuration(((total - fe) / fe) * elapsed)}`
}

onMounted(() => {
  void refreshAll().finally(() => { initialLoading.value = false })
  taskRefreshTimer = window.setInterval(() => void refreshDashboard(), 5000)
})
onBeforeUnmount(() => {
  stopResize()
  for (const timer of toastTimers.values()) window.clearTimeout(timer)
  toastTimers.clear()
  if (taskRefreshTimer) window.clearInterval(taskRefreshTimer)
})
</script>

<template>
  <header class="app-header">
    <div class="brand"><Server :size="21" /><span>PlatEMO HPC</span><small>调度中心</small></div>
    <div class="header-status"><button class="header-button" type="button" @click="showPlatemoForm = true"><FolderCog :size="16" /> PlatEMO 路径</button><span class="status-dot"></span>{{ workers.filter((worker) => worker.online).length }} 个节点在线</div>
  </header>

  <Transition name="loading-overlay"><div v-if="initialLoading" class="loading-overlay" role="status" aria-live="polite"><div class="loading-indicator"><Cog :size="30" /><span>正在加载调度数据</span></div></div></Transition>

  <main ref="workspace" class="workspace" :style="workspaceStyle">
    <aside class="catalog-panel" :style="catalogStyle">
      <section class="catalog-section catalog-pane">
        <div class="section-title">可用算法 <label class="search-field"><Search :size="15" /><input v-model="algorithmSearch" placeholder="搜索算法" /></label></div>
        <div class="catalog-list" role="listbox">
          <button v-for="item in filteredAlgorithms" :key="item.name" class="catalog-item" type="button" @click="addAlgorithm(item)">{{ item.name }}</button>
        </div>
      </section>
      <div class="row-resizer" title="拖拽调整算法与问题区域高度" @pointerdown="startCatalogResize('algorithms', $event)"><span></span></div>

      <section class="catalog-section catalog-pane">
        <div class="section-title">可用问题 <label class="search-field"><Search :size="15" /><input v-model="problemSearch" placeholder="搜索问题" /></label></div>
        <div class="catalog-list" role="listbox">
          <button v-for="item in filteredProblems" :key="item.name" class="catalog-item" type="button" @click="addProblem(item)">{{ item.name }}</button>
        </div>
      </section>
      <div class="row-resizer" title="拖拽调整问题与执行设置区域高度" @pointerdown="startCatalogResize('problems', $event)"><span></span></div>

      <section class="catalog-section catalog-pane settings-section">
        <div class="section-title">执行设置</div>
        <label class="setting-field"><span>每个测试点运行次数</span><input v-model.number="runs" min="1" max="1000" type="number" /></label>
        <label class="setting-field"><span>每次运行保留数据点</span><input v-model.number="retainPoints" min="1" type="number" /></label>
        <label class="setting-field"><span>MATLAB Profile</span><input v-model="clusterProfile" placeholder="使用 Worker 默认 Profile" /></label>
        <label class="setting-field"><span>PlatEMO Commit</span><input v-model="requiredPlatemoCommit" placeholder="可留空" /></label>
        <label class="setting-field"><span>最小可用磁盘 (bytes)</span><input v-model.number="minimumDiskFreeBytes" min="0" type="number" placeholder="0" /></label>
      </section>

      <section class="catalog-section catalog-pane worker-section">
        <div class="section-title">Worker 管理
          <span class="title-actions"><button title="同步 Worker 状态" class="icon-button" type="button" :disabled="workersRefreshing" @click="refreshWorkers"><RefreshCw :size="16" /></button><button title="添加 Worker" class="icon-button" type="button" @click="openAddWorker"><Plus :size="17" /></button></span>
        </div>
        <div class="worker-list">
          <article v-for="worker in workers" :key="worker.id" class="worker-row worker-row-unchecked" :class="{ offline: !worker.online }">
            <span class="worker-name"><strong>{{ worker.name }}</strong><small><b class="worker-status-badge" :class="`worker-status-${workerStatus(worker).tone}`">{{ workerStatus(worker).label }}</b><span class="worker-pool-usage">池 {{ workerPoolUsage(worker) }}</span><span class="worker-heartbeat">{{ heartbeatAge(worker) }}</span><button :title="Number(worker.dispatch_paused ?? 0) ? '恢复 Worker 接单' : '暂停 Worker 接单'" class="icon-button" type="button" :disabled="workerPauseChanging === worker.id" @click.prevent="toggleWorkerDispatchPause(worker)"><Play v-if="Number(worker.dispatch_paused ?? 0)" :size="15" /><Pause v-else :size="15" /></button><button title="编辑 Worker" class="icon-button" type="button" @click.prevent="openEditWorker(worker)"><Pencil :size="15" /></button><button title="删除 Worker" class="icon-button danger" type="button" @click.prevent="deleteWorker(worker)"><Trash2 :size="15" /></button></small><em v-if="workerStatus(worker).detail">{{ workerStatus(worker).detail }}</em></span>
          </article>
          <p v-if="!workers.length" class="empty-text">暂无 Worker，点击右上角添加。</p>
        </div>
      </section>
    </aside>
    <div class="column-resizer" title="拖拽调整左列宽度" @pointerdown="startColumnResize('catalog', $event)"><span></span></div>

    <section class="editor-panel">
      <div class="editor-heading"><h1>运行设置</h1><div class="settings-actions"><button class="outline-button" type="button" @click="uploadInput?.click()"><FileUp :size="16" /> 导入</button><button class="outline-button" type="button" @click="saveNativeSetting"><Save :size="16" /> 保存</button><input ref="uploadInput" class="visually-hidden" type="file" accept=".mat" @change="importFile" /></div></div>
      <p v-if="importedDiagnostics.length" class="warning">{{ importedDiagnostics.join('；') }}</p>
      <div class="editor-scroll">
        <h2>已选算法</h2>
        <article v-for="item in selectedAlgorithms" :key="item.id" class="selected-card algorithm-card">
          <div class="card-name"><strong>{{ item.name }}</strong><button title="移除算法" class="icon-button" type="button" @click="removeItem(selectedAlgorithms, item.id)"><X :size="16" /></button></div>
          <div v-if="schema(item, 'algorithm').length" class="parameter-grid"><label v-for="parameter in schema(item, 'algorithm')" :key="parameter.name"><span>{{ parameter.name }}</span><input v-model="item.parameters[parameter.name]" /></label></div>
        </article>
        <p v-if="!selectedAlgorithms.length" class="empty-text">点击左侧算法加入运行列表。</p>

        <h2>已选问题</h2>
        <article v-for="item in selectedProblems" :key="item.id" class="selected-card problem-card">
          <div class="card-name"><strong>{{ item.name }}</strong><button title="移除问题" class="icon-button" type="button" @click="removeItem(selectedProblems, item.id)"><X :size="16" /></button></div>
          <div class="parameter-grid"><label v-for="parameter in schema(item, 'problem')" :key="parameter.name"><span>{{ parameter.name }}</span><input v-model="item.parameters[parameter.name]" /></label></div>
        </article>
        <p v-if="!selectedProblems.length" class="empty-text">问题可重复加入，以建立不同参数规模的测试实例。</p>
      </div>
    </section>
    <div class="column-resizer" title="拖拽调整中间列宽度" @pointerdown="startColumnResize('editor', $event)"><span></span></div>

    <section class="task-panel">
      <div class="plan-toolbar"><button class="primary-button" type="button" :disabled="busy" @click="startRun">启动任务</button><button class="outline-button" type="button" :disabled="busy" @click="refreshAll"><RefreshCw :size="16" /> 刷新目录</button></div>
      <section class="plan-card schedule-card"><div class="schedule-tabs" role="tablist" aria-label="任务队列视图"><button class="schedule-tab" :class="{ active: scheduleView === 'queue' }" type="button" role="tab" :aria-selected="scheduleView === 'queue'" @click="scheduleView = 'queue'">计划分配 <small>{{ pendingTasks.length }}</small></button><button class="schedule-tab" :class="{ active: scheduleView === 'history' }" type="button" role="tab" :aria-selected="scheduleView === 'history'" @click="scheduleView = 'history'">任务记录 <small>{{ taskHistory.length }}</small></button><span class="panel-actions"><button v-if="scheduleView === 'queue'" class="icon-button" type="button" :title="queueExpanded ? '收起待分配列表' : '展开待分配列表'" @click="queueExpanded = !queueExpanded"><ChevronUp v-if="queueExpanded" :size="16" /><ChevronDown v-else :size="16" /></button><button v-else class="icon-button" type="button" :title="historyExpanded ? '收起任务记录' : '展开任务记录'" @click="historyExpanded = !historyExpanded"><ChevronUp v-if="historyExpanded" :size="16" /><ChevronDown v-else :size="16" /></button><button class="text-button" type="button" :disabled="busy" @click="toggleDispatchPause()">{{ dispatchPaused ? '恢复分配' : '暂停分配' }}</button><button class="text-button danger" type="button" :disabled="busy" @click="cancelAllTasks">取消所有任务</button></span></div><div class="plan-metrics-inline"><span><strong>{{ pendingTasks.length }}</strong> 待分配</span><i aria-hidden="true"></i><span><strong>{{ claimedTaskCount }}</strong> 已接取</span><i aria-hidden="true"></i><span><strong>{{ runningTaskCount }}</strong> 运行中</span><i aria-hidden="true"></i><span><strong>{{ availableWorkerCount }}</strong> 可接单节点</span></div><div v-if="scheduleView === 'queue' && queueExpanded" class="assignment-list"><div class="list-toolbar"><span>第 {{ queuePage }} / {{ queuePageCount }} 页，共 {{ pendingTasks.length }} 条</span><span class="panel-actions"><label>每页 <select v-model.number="queuePageSize"><option :value="20">20</option><option :value="50">50</option><option :value="100">100</option><option :value="500">500</option></select> 条</label></span></div><div v-for="task in pagedPendingTasks" :key="task.id" class="assignment-row"><div class="assignment-main"><strong>{{ task.problem }}</strong><span>{{ task.algorithm }}</span><small>{{ parameterSummary(task) }}</small></div><div class="assignment-actions"><button class="text-button danger" type="button" :disabled="busy" @click="cancelPendingTask(task)">取消</button></div></div><div v-if="pendingTasks.length" class="history-pagination"><button class="icon-button" type="button" title="上一页" :disabled="queuePage <= 1" @click="queuePage -= 1">‹</button><span>第 {{ queuePage }} / {{ queuePageCount }} 页，共 {{ pendingTasks.length }} 条</span><button class="icon-button" type="button" title="下一页" :disabled="queuePage >= queuePageCount" @click="queuePage += 1">›</button></div><p v-if="!pendingTasks.length" class="empty-text">当前没有待分配任务。</p></div><div v-else-if="scheduleView === 'history' && historyExpanded" class="task-history-list"><div class="list-toolbar"><label><input type="checkbox" :checked="currentPageHistorySelected" :disabled="!pagedTaskHistory.length || busy" @change="toggleCurrentHistoryPage"> 全选当前页</label><span class="panel-actions"><label>每页 <select v-model.number="historyPageSize"><option :value="20">20</option><option :value="50">50</option><option :value="100">100</option><option :value="500">500</option></select> 条</label><button v-if="selectedHistoryKeys.length" class="text-button danger" type="button" :disabled="busy" @click="deleteSelectedTaskHistories">删除所选 ({{ selectedHistoryKeys.length }})</button></span></div><article v-for="task in pagedTaskHistory" :key="task.id" class="task-history-row"><input v-model="selectedHistoryKeys" type="checkbox" :value="historyTaskKey(task)" :disabled="busy" :aria-label="`选择 ${task.problem} Seed ${task.seed}`"><span class="task-state-dot" :class="task.state"></span><strong>{{ task.problem }}</strong><span>{{ task.algorithm }}</span><small>Seed {{ task.seed }} · {{ taskStateLabel(task) }} · {{ task.updated_at }}</small><button class="icon-button danger" type="button" title="删除任务记录" :disabled="busy" @click="deleteTaskHistory(task)"><Trash2 :size="15" /></button></article><div v-if="taskHistory.length" class="history-pagination"><button class="icon-button" type="button" title="上一页" :disabled="historyPage <= 1" @click="historyPage -= 1">‹</button><span>第 {{ historyPage }} / {{ historyPageCount }} 页，共 {{ taskHistory.length }} 条</span><button class="icon-button" type="button" title="下一页" :disabled="historyPage >= historyPageCount" @click="historyPage += 1">›</button></div><p v-if="!taskHistory.length" class="empty-text">完成和失败的任务将在此保留记录。</p></div></section>
      <section class="plan-card task-card"><h2 class="panel-heading">Seed 运行 <small>{{ activeTaskCount }} 个活动任务</small><span class="panel-actions"><button class="text-button danger" type="button" :disabled="busy" @click="cancelAllTasks">停止所有任务</button></span></h2><div class="task-list"><article v-for="task in seedRunTasks" :key="task.id" class="task-row"><div class="task-node"><span class="task-state-dot" :class="task.state"></span><strong>{{ task.worker_name }}</strong><span>{{ taskStateLabel(task) }}</span><button v-if="isCancellableTask(task)" class="icon-button task-cancel" type="button" :title="task.attempt_kind === 'dynamic_seed' ? '取消此 Seed' : '取消此 Worker 批次'" :disabled="busy" @click="cancelTaskBatch(task)"><CircleStop :size="16" /></button></div><div class="task-run"><div class="task-identifiers"><strong>{{ task.problem }}</strong><span>{{ task.algorithm }}</span></div><div class="progress-track" :aria-label="taskFeLabel(task)"><i :style="{ width: `${taskProgress(task)}%` }"></i></div><div class="task-metrics"><strong>{{ taskFeLabel(task) }}</strong><span>{{ taskProgress(task) }}%</span><small>运行 {{ formatDuration(task.elapsed_seconds) }}</small><small>{{ taskEtaLabel(task) }}</small></div></div><div class="task-meta">Seed {{ task.seed }} · N{{ taskParameter(task, 'N') }} · M{{ taskParameter(task, 'M') }} · D{{ taskParameter(task, 'D') }}</div></article><p v-if="!seedRunTasks.length" class="empty-text">Worker 领取任务后将在此显示每个 Seed 的分配和进度。</p></div></section>
    </section>
    <div class="column-resizer" title="拖拽调整已有数据列宽度" @pointerdown="startColumnResize('existing', $event)"><span></span></div>

    <section class="existing-panel">
      <section class="plan-card existing-card"><h2>已有数据 <small>已选 {{ selectedExistingTestKeys.length }}</small></h2><div class="existing-list"><label v-for="test in existingTests" :key="existingTestKey(test)" class="existing-row" :class="{ selected: selectedExistingTestKeys.includes(existingTestKey(test)) }"><input v-model="selectedExistingTestKeys" :value="existingTestKey(test)" type="checkbox" /><span><span class="existing-title"><strong>{{ test.problem }}</strong><b>{{ test.algorithm }}</b></span><small>N— · M{{ test.M }} · D{{ test.D }}</small></span><button title="按此规模加入问题实例" class="icon-button" type="button" @click.prevent="addExistingTest(test)"><Plus :size="16" /></button></label><p v-if="!existingTests.length" class="empty-text">当前 Data 目录中没有可识别的结果文件。</p></div></section>
    </section>
  </main>

  <div v-if="showWorkerForm" class="modal-backdrop" @click.self="showWorkerForm = false"><form class="worker-dialog" @submit.prevent="saveWorker"><div class="dialog-heading"><h2>{{ editingWorker ? '编辑 Worker' : '手动添加 Worker' }}</h2><button title="关闭" class="icon-button" type="button" @click="showWorkerForm = false"><X :size="18" /></button></div><label>名称<input v-model.trim="workerForm.name" required /></label><label>地址<input v-model.trim="workerForm.url" required placeholder="http://zerotier-ip:6001" /></label><label>Node Token<input v-model="workerForm.token" type="password" :placeholder="editingWorker ? '留空则不修改' : '注册后从 Worker 配置复制'" /></label><label>优先级<input v-model.number="workerForm.priority" type="number" min="0" /></label><p class="dialog-note">推荐直接在 Worker 配置 Master Join Token，让 Worker 自动注册；手动添加仅用于已取得 Node Token 的节点。</p><div class="dialog-actions"><button class="outline-button" type="button" @click="showWorkerForm = false">取消</button><button class="primary-button" :disabled="busy" type="submit">保存</button></div></form></div>
  <div v-if="showPlatemoForm" class="modal-backdrop" @click.self="showPlatemoForm = false"><form class="worker-dialog" @submit.prevent="savePlatemoPath"><div class="dialog-heading"><h2>PlatEMO 路径</h2><button title="关闭" class="icon-button" type="button" @click="showPlatemoForm = false"><X :size="18" /></button></div><label>根目录<input v-model.trim="platemoPath" required placeholder="例如：H:\PlatEMO" /></label><p class="dialog-note">目录必须包含 Algorithms、Problems 和 Data。</p><div class="dialog-actions"><button class="outline-button" type="button" @click="showPlatemoForm = false">取消</button><button class="primary-button" :disabled="busy" type="submit">保存并解析</button></div></form></div>
  <TransitionGroup name="toast-stack" tag="div" class="toast-stack"><div v-for="toast in toasts" :key="toast.id" class="toast" :class="`${toast.kind}-toast`"><span>{{ toast.message }}</span><i class="toast-progress"></i></div></TransitionGroup>
</template>
