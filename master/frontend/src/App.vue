<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { FileUp, FolderCog, Pencil, Plus, RefreshCw, Save, Search, Server, Trash2, X } from '@lucide/vue'
import { api } from './api'
import type { CatalogItem, ExistingTest, ExperimentItem, ImportedSettings, TaskStatus, Worker } from './types'

const algorithms = ref<CatalogItem[]>([])
const problems = ref<CatalogItem[]>([])
const existingTests = ref<ExistingTest[]>([])
const selectedExistingTestKeys = ref<string[]>([])
const taskStatuses = ref<TaskStatus[]>([])
const workers = ref<Worker[]>([])
const selectedAlgorithms = ref<ExperimentItem[]>([])
const selectedProblems = ref<ExperimentItem[]>([])
const checkedWorkers = ref<string[]>([])
const algorithmSearch = ref('')
const problemSearch = ref('')
const platemoPath = ref('')
const runs = ref(30)
const maxWorkers = ref<number | null>(null)
const retainPoints = ref(100)
const showWorkerForm = ref(false)
const showPlatemoForm = ref(false)
const editingWorker = ref<string | null>(null)
const workerForm = ref({ name: '', url: '', token: '', priority: 0 })
const busy = ref(false)
const notice = ref<{ id: number; message: string } | null>(null)
const error = ref('')
const importedDiagnostics = ref<string[]>([])
const uploadInput = ref<HTMLInputElement | null>(null)
const workspace = ref<HTMLElement | null>(null)
const columnWidths = ref({ catalog: 330, editor: 370, existing: 330 })
const catalogHeights = ref({ algorithms: 270, problems: 270 })
let noticeId = 0
let noticeTimer: ReturnType<typeof window.setTimeout> | undefined
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
const selectedWorkerCount = computed(() => checkedWorkers.value.length)
const activeTaskCount = computed(() => taskStatuses.value.filter((task) => !['completed', 'failed', 'cancelled'].includes(task.state)).length)

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
  if (noticeTimer) window.clearTimeout(noticeTimer)
  notice.value = { id: ++noticeId, message }
  noticeTimer = window.setTimeout(() => {
    notice.value = null
    noticeTimer = undefined
  }, 4600)
}

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
      columnWidths.value.editor = clamp(state.widths.editor + delta, 330, width - state.widths.catalog - state.widths.existing - minimumTaskWidth - handleWidth)
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
  retainPoints.value = Number(values.retain_results) || 1
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
}

async function refreshWorkers() {
  busy.value = true
  error.value = ''
  try {
    await api.probeWorkers()
    workers.value = await api.workers()
    checkedWorkers.value = checkedWorkers.value.filter((id) => workers.value.some((worker) => worker.id === id && worker.online))
  } catch (caught) {
    error.value = caught instanceof Error ? caught.message : '刷新 Worker 失败'
  } finally {
    busy.value = false
  }
}

async function refreshAll() {
  busy.value = true
  error.value = ''
  try {
    await loadCatalog()
    workers.value = await api.workers()
    await loadTasks()
  } catch (caught) {
    error.value = caught instanceof Error ? caught.message : '读取 Master 数据失败'
  } finally {
    busy.value = false
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
      execution: { runs: runs.value, max_workers: maxWorkers.value, retain_points: retainPoints.value },
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
  try {
    if (editingWorker.value) await api.editWorker(editingWorker.value, workerForm.value)
    else await api.addWorker(workerForm.value)
    showWorkerForm.value = false
    await refreshWorkers()
  } catch (caught) {
    error.value = caught instanceof Error ? caught.message : '保存 Worker 失败'
  } finally {
    busy.value = false
  }
}

async function deleteWorker(worker: Worker) {
  if (!window.confirm(`删除 Worker “${worker.name}”？`)) return
  try {
    await api.deleteWorker(worker.id)
    await refreshWorkers()
  } catch (caught) {
    error.value = caught instanceof Error ? caught.message : '删除 Worker 失败'
  }
}

async function startRun() {
  error.value = ''
  if (!selectedAlgorithms.value.length || !selectedProblems.value.length || !checkedWorkers.value.length) {
    error.value = '需要至少选择一个算法、一个问题实例和一个在线 Worker。'
    return
  }
  busy.value = true
  try {
    await api.startRun({
      algorithms: selectedAlgorithms.value,
      problems: selectedProblems.value,
      runs: runs.value,
      maxWorkers: maxWorkers.value,
      retainPoints: retainPoints.value,
      workerIds: checkedWorkers.value,
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
  void refreshAll()
  void loadTasks()
  taskRefreshTimer = window.setInterval(() => void loadTasks(), 5000)
})
onBeforeUnmount(() => {
  stopResize()
  if (noticeTimer) window.clearTimeout(noticeTimer)
  if (taskRefreshTimer) window.clearInterval(taskRefreshTimer)
})
</script>

<template>
  <header class="app-header">
    <div class="brand"><Server :size="21" /><span>PlatEMO HPC</span><small>调度中心</small></div>
    <div class="header-status"><button class="header-button" type="button" @click="showPlatemoForm = true"><FolderCog :size="16" /> PlatEMO 路径</button><span class="status-dot"></span>{{ workers.filter((worker) => worker.online).length }} 个节点在线</div>
  </header>

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
        <label class="setting-field"><span>单个任务最大 Worker 数</span><input v-model.number="maxWorkers" min="1" type="number" placeholder="不限" /></label>
        <label class="setting-field"><span>每次运行保留数据点</span><input v-model.number="retainPoints" min="1" type="number" /></label>
      </section>

      <section class="catalog-section catalog-pane worker-section">
        <div class="section-title">Worker 选择
          <span class="title-actions"><button title="探测全部 Worker" class="icon-button" type="button" :disabled="busy" @click="refreshWorkers"><RefreshCw :size="16" /></button><button title="添加 Worker" class="icon-button" type="button" @click="openAddWorker"><Plus :size="17" /></button></span>
        </div>
        <div class="worker-list">
          <label v-for="worker in workers" :key="worker.id" class="worker-row" :class="{ offline: !worker.online }">
            <input v-model="checkedWorkers" :value="worker.id" :disabled="!worker.online" type="checkbox" />
            <span class="worker-state" :class="worker.online ? 'online' : 'offline'"></span>
            <span class="worker-name">{{ worker.name }}<small>{{ worker.online ? `在线 · 队列 ${worker.queue_count ?? 0}` : '离线' }}</small></span>
            <button title="编辑 Worker" class="icon-button" type="button" @click.prevent="openEditWorker(worker)"><Pencil :size="15" /></button>
            <button title="删除 Worker" class="icon-button danger" type="button" @click.prevent="deleteWorker(worker)"><Trash2 :size="15" /></button>
          </label>
          <p v-if="!workers.length" class="empty-text">暂无 Worker，点击右上角添加。</p>
        </div>
      </section>
    </aside>
    <div class="column-resizer" title="拖拽调整左列宽度" @pointerdown="startColumnResize('catalog', $event)"><span></span></div>

    <section class="editor-panel">
      <div class="editor-heading"><h1>运行列表与参数设置</h1><div class="settings-actions"><button class="outline-button" type="button" @click="uploadInput?.click()"><FileUp :size="16" /> 导入</button><button class="outline-button" type="button" @click="saveNativeSetting"><Save :size="16" /> 保存</button><input ref="uploadInput" class="visually-hidden" type="file" accept=".mat" @change="importFile" /></div></div>
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
      <section class="plan-card"><h2>计划分配</h2><div class="metric-grid"><div><strong>{{ totalTasks }}</strong><span>总任务</span></div><div><strong>{{ selectedAlgorithms.length }}</strong><span>算法</span></div><div><strong>{{ selectedProblems.length }}</strong><span>问题</span></div><div><strong>{{ selectedWorkerCount }}</strong><span>节点</span></div></div><p class="plan-note">{{ selectedAlgorithms.length }} 个算法 × {{ selectedProblems.length }} 个问题实例 × {{ runs }} 次运行</p></section>
      <section class="plan-card task-card"><h2>Seed 运行 <small>{{ activeTaskCount }} 个活动任务</small></h2><div class="task-list"><article v-for="task in taskStatuses" :key="task.id" class="task-row"><div class="task-node"><span class="task-state-dot" :class="task.state"></span><strong>{{ task.worker_name }}</strong><span>{{ taskStateLabel(task) }}</span></div><div class="task-run"><div class="task-identifiers"><strong>{{ task.problem }}</strong><span>{{ task.algorithm }}</span></div><div class="progress-track" :aria-label="taskFeLabel(task)"><i :style="{ width: `${taskProgress(task)}%` }"></i></div><div class="task-metrics"><strong>{{ taskFeLabel(task) }}</strong><span>{{ taskProgress(task) }}%</span><small>运行 {{ formatDuration(task.elapsed_seconds) }}</small><small>{{ taskEtaLabel(task) }}</small></div></div><div class="task-meta">Seed {{ task.seed }} · N{{ taskParameter(task, 'N') }} · M{{ taskParameter(task, 'M') }} · D{{ taskParameter(task, 'D') }}</div></article><p v-if="!taskStatuses.length" class="empty-text">启动任务后将在此显示每个 Seed 的分配和进度。</p></div></section>
    </section>
    <div class="column-resizer" title="拖拽调整已有数据列宽度" @pointerdown="startColumnResize('existing', $event)"><span></span></div>

    <section class="existing-panel">
      <section class="plan-card existing-card"><h2>已有数据 <small>已选 {{ selectedExistingTestKeys.length }}</small></h2><div class="existing-list"><label v-for="test in existingTests" :key="existingTestKey(test)" class="existing-row" :class="{ selected: selectedExistingTestKeys.includes(existingTestKey(test)) }"><input v-model="selectedExistingTestKeys" :value="existingTestKey(test)" type="checkbox" /><span><span class="existing-title"><strong>{{ test.problem }}</strong><b>{{ test.algorithm }}</b></span><small>N— · M{{ test.M }} · D{{ test.D }}</small></span><button title="按此规模加入问题实例" class="icon-button" type="button" @click.prevent="addExistingTest(test)"><Plus :size="16" /></button></label><p v-if="!existingTests.length" class="empty-text">当前 Data 目录中没有可识别的结果文件。</p></div></section>
    </section>
  </main>

  <div v-if="showWorkerForm" class="modal-backdrop" @click.self="showWorkerForm = false"><form class="worker-dialog" @submit.prevent="saveWorker"><div class="dialog-heading"><h2>{{ editingWorker ? '编辑 Worker' : '手动添加 Worker' }}</h2><button title="关闭" class="icon-button" type="button" @click="showWorkerForm = false"><X :size="18" /></button></div><label>名称<input v-model.trim="workerForm.name" required /></label><label>地址<input v-model.trim="workerForm.url" required placeholder="http://zerotier-ip:6001" /></label><label>Node Token<input v-model="workerForm.token" type="password" :placeholder="editingWorker ? '留空则不修改' : '注册后从 Worker 配置复制'" /></label><label>优先级<input v-model.number="workerForm.priority" type="number" min="0" /></label><p class="dialog-note">推荐直接在 Worker 配置 Master Join Token，让 Worker 自动注册；手动添加仅用于已取得 Node Token 的节点。</p><div class="dialog-actions"><button class="outline-button" type="button" @click="showWorkerForm = false">取消</button><button class="primary-button" :disabled="busy" type="submit">保存</button></div></form></div>
  <div v-if="showPlatemoForm" class="modal-backdrop" @click.self="showPlatemoForm = false"><form class="worker-dialog" @submit.prevent="savePlatemoPath"><div class="dialog-heading"><h2>PlatEMO 路径</h2><button title="关闭" class="icon-button" type="button" @click="showPlatemoForm = false"><X :size="18" /></button></div><label>根目录<input v-model.trim="platemoPath" required placeholder="例如：H:\PlatEMO" /></label><p class="dialog-note">目录必须包含 Algorithms、Problems 和 Data。</p><div class="dialog-actions"><button class="outline-button" type="button" @click="showPlatemoForm = false">取消</button><button class="primary-button" :disabled="busy" type="submit">保存并解析</button></div></form></div>
  <Transition name="toast"><div v-if="notice" :key="notice.id" class="toast notice-toast"><span>{{ notice.message }}</span><i class="toast-progress"></i></div></Transition><div v-if="error" class="toast error-toast">{{ error }}</div>
</template>
