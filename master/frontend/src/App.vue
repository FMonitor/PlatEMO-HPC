<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { FileUp, FolderCog, Pencil, Plus, RefreshCw, Save, Search, Server, Trash2, X } from '@lucide/vue'
import { api } from './api'
import type { CatalogItem, ExistingTest, ExperimentItem, ImportedSettings, Worker } from './types'

const algorithms = ref<CatalogItem[]>([])
const problems = ref<CatalogItem[]>([])
const settings = ref<{ filename: string; name: string }[]>([])
const existingTests = ref<ExistingTest[]>([])
const selectedExistingTestKeys = ref<string[]>([])
const workers = ref<Worker[]>([])
const selectedAlgorithms = ref<ExperimentItem[]>([])
const selectedProblems = ref<ExperimentItem[]>([])
const checkedWorkers = ref<string[]>([])
const algorithmSearch = ref('')
const problemSearch = ref('')
const selectedSetting = ref('')
const platemoPath = ref('')
const runs = ref(30)
const maxWorkers = ref<number | null>(null)
const retainPoints = ref(100)
const showWorkerForm = ref(false)
const showPlatemoForm = ref(false)
const editingWorker = ref<string | null>(null)
const workerForm = ref({ name: '', url: '', token: '' })
const busy = ref(false)
const notice = ref('')
const error = ref('')
const importedDiagnostics = ref<string[]>([])
const uploadInput = ref<HTMLInputElement | null>(null)

const filteredAlgorithms = computed(() => filterCatalog(algorithms.value, algorithmSearch.value))
const filteredProblems = computed(() => filterCatalog(problems.value, problemSearch.value))
const totalTasks = computed(() => selectedAlgorithms.value.length * selectedProblems.value.length * runs.value)
const selectedWorkerCount = computed(() => checkedWorkers.value.length)

function existingTestKey(test: ExistingTest) {
  return `${test.algorithm}:${test.problem}:M${test.M}:D${test.D}`
}

function filterCatalog(items: CatalogItem[], query: string) {
  const normalized = query.trim().toLowerCase()
  return normalized ? items.filter((item) => item.name.toLowerCase().includes(normalized)) : items
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
  notice.value = `已加载 ${values.source}：${values.algorithms.length} 个算法、${values.problems.length} 个问题实例。`
}

async function loadCatalog() {
  const catalog = await api.catalog()
  algorithms.value = catalog.algorithms
  problems.value = catalog.problems
  settings.value = catalog.settings
  existingTests.value = catalog.existing_tests
  platemoPath.value = catalog.platemo_path
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
    notice.value = 'PlatEMO 目录已解析。'
  } catch (caught) {
    error.value = caught instanceof Error ? caught.message : '设置 PlatEMO 路径失败'
  } finally {
    busy.value = false
  }
}

async function previewSelectedSetting() {
  if (!selectedSetting.value) return
  busy.value = true
  error.value = ''
  try {
    applyImported(await api.previewSetting(selectedSetting.value))
  } catch (caught) {
    error.value = caught instanceof Error ? caught.message : '读取预设失败'
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
    notice.value = '已导出 Master 原生设置文件。'
  } catch (caught) {
    error.value = caught instanceof Error ? caught.message : '保存设置失败'
  }
}

function openAddWorker() {
  workerForm.value = { name: '', url: '', token: '' }
  editingWorker.value = null
  showWorkerForm.value = true
}

function openEditWorker(worker: Worker) {
  workerForm.value = { name: worker.name, url: worker.url, token: '' }
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

async function startMock() {
  error.value = ''
  if (!selectedAlgorithms.value.length || !selectedProblems.value.length || !checkedWorkers.value.length) {
    error.value = '需要至少选择一个算法、一个问题实例和一个在线 Worker。'
    return
  }
  busy.value = true
  try {
    await api.startMock({
      algorithms: selectedAlgorithms.value,
      problems: selectedProblems.value,
      runs: runs.value,
      maxWorkers: maxWorkers.value,
      retainPoints: retainPoints.value,
      workerIds: checkedWorkers.value,
    })
    notice.value = `已创建 ${totalTasks.value} 个模拟任务。`
  } catch (caught) {
    error.value = caught instanceof Error ? caught.message : '创建模拟任务失败'
  } finally {
    busy.value = false
  }
}

onMounted(refreshAll)
</script>

<template>
  <header class="app-header">
    <div class="brand"><Server :size="21" /><span>PlatEMO HPC</span><small>调度中心</small></div>
    <div class="header-status"><button class="header-button" type="button" @click="showPlatemoForm = true"><FolderCog :size="16" /> PlatEMO 路径</button><span class="status-dot"></span>{{ workers.filter((worker) => worker.online).length }} 个节点在线</div>
  </header>

  <main class="workspace">
    <aside class="catalog-panel">
      <section class="catalog-section">
        <div class="section-title">可用算法 <label class="search-field"><Search :size="15" /><input v-model="algorithmSearch" placeholder="搜索算法" /></label></div>
        <div class="catalog-list" role="listbox">
          <button v-for="item in filteredAlgorithms" :key="item.name" class="catalog-item" type="button" @click="addAlgorithm(item)">{{ item.name }}</button>
        </div>
      </section>

      <section class="catalog-section">
        <div class="section-title">可用问题 <label class="search-field"><Search :size="15" /><input v-model="problemSearch" placeholder="搜索问题" /></label></div>
        <div class="catalog-list" role="listbox">
          <button v-for="item in filteredProblems" :key="item.name" class="catalog-item" type="button" @click="addProblem(item)">{{ item.name }}</button>
        </div>
      </section>

      <section class="catalog-section settings-section">
        <div class="section-title">执行设置</div>
        <label class="setting-field"><span>每个测试点运行次数</span><input v-model.number="runs" min="1" max="1000" type="number" /></label>
        <label class="setting-field"><span>单个任务最大 Worker 数</span><input v-model.number="maxWorkers" min="1" type="number" placeholder="不限" /></label>
        <label class="setting-field"><span>每次运行保留数据点</span><input v-model.number="retainPoints" min="1" type="number" /></label>
      </section>

      <section class="catalog-section worker-section">
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

    <section class="editor-panel">
      <div class="editor-heading"><h1>运行列表与参数设置</h1><div class="settings-actions"><button class="outline-button" type="button" @click="uploadInput?.click()"><FileUp :size="16" /> 导入</button><button class="outline-button" type="button" @click="saveNativeSetting"><Save :size="16" /> 保存</button><input ref="uploadInput" class="visually-hidden" type="file" accept=".mat" @change="importFile" /></div></div>
      <div class="preset-row"><label>实验预设<select v-model="selectedSetting" @change="previewSelectedSetting"><option value="">不使用预设</option><option v-for="setting in settings" :key="setting.filename" :value="setting.filename">{{ setting.name }}</option></select></label><span>导入 PlatEMO `Setting*.mat`；保存为 Master 原生 MAT。</span></div>
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

    <section class="plan-panel">
      <div class="plan-toolbar"><button class="primary-button" type="button" :disabled="busy" @click="startMock">启动模拟任务</button><button class="outline-button" type="button" :disabled="busy" @click="refreshAll"><RefreshCw :size="16" /> 刷新目录</button></div>
      <section class="plan-card"><h2>结果展示：计划分配</h2><div class="metric-grid"><div><strong>{{ totalTasks }}</strong><span>总任务</span></div><div><strong>{{ selectedAlgorithms.length }}</strong><span>算法</span></div><div><strong>{{ selectedProblems.length }}</strong><span>问题实例</span></div><div><strong>{{ selectedWorkerCount }}</strong><span>已选节点</span></div></div><p class="plan-note">{{ selectedAlgorithms.length }} 个算法 × {{ selectedProblems.length }} 个问题实例 × {{ runs }} 次运行</p></section>
      <section class="plan-card existing-card"><h2>已有数据测试 <small>已选 {{ selectedExistingTestKeys.length }}</small></h2><div class="existing-list"><label v-for="test in existingTests" :key="existingTestKey(test)" class="existing-row" :class="{ selected: selectedExistingTestKeys.includes(existingTestKey(test)) }"><input v-model="selectedExistingTestKeys" :value="existingTestKey(test)" type="checkbox" /><span><strong>{{ test.problem }}</strong><small>{{ test.algorithm }} · M{{ test.M }} · D{{ test.D }}</small></span><b>{{ test.run_count }} 次</b><button title="按此规模加入问题实例" class="icon-button" type="button" @click.prevent="addExistingTest(test)"><Plus :size="16" /></button></label><p v-if="!existingTests.length" class="empty-text">当前 Data 目录中没有可识别的结果文件。</p></div></section>
    </section>
  </main>

  <div v-if="showWorkerForm" class="modal-backdrop" @click.self="showWorkerForm = false"><form class="worker-dialog" @submit.prevent="saveWorker"><div class="dialog-heading"><h2>{{ editingWorker ? '编辑 Worker' : '添加 Worker' }}</h2><button title="关闭" class="icon-button" type="button" @click="showWorkerForm = false"><X :size="18" /></button></div><label>名称<input v-model.trim="workerForm.name" required /></label><label>地址<input v-model.trim="workerForm.url" required placeholder="http://zerotier-ip:6001" /></label><label>Token<input v-model="workerForm.token" type="password" :placeholder="editingWorker ? '留空则不修改' : '可选'" /></label><div class="dialog-actions"><button class="outline-button" type="button" @click="showWorkerForm = false">取消</button><button class="primary-button" :disabled="busy" type="submit">保存</button></div></form></div>
  <div v-if="showPlatemoForm" class="modal-backdrop" @click.self="showPlatemoForm = false"><form class="worker-dialog" @submit.prevent="savePlatemoPath"><div class="dialog-heading"><h2>PlatEMO 路径</h2><button title="关闭" class="icon-button" type="button" @click="showPlatemoForm = false"><X :size="18" /></button></div><label>根目录<input v-model.trim="platemoPath" required placeholder="例如：H:\PlatEMO" /></label><p class="dialog-note">目录必须包含 Algorithms、Problems 和 Data。</p><div class="dialog-actions"><button class="outline-button" type="button" @click="showPlatemoForm = false">取消</button><button class="primary-button" :disabled="busy" type="submit">保存并解析</button></div></form></div>
  <div v-if="notice" class="toast notice-toast">{{ notice }}</div><div v-if="error" class="toast error-toast">{{ error }}</div>
</template>
