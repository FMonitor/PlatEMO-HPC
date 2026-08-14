import type { CatalogResponse, ImportedSettings, TaskStatus, Worker } from './types'

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, init)
  if (!response.ok) {
    const detail = await response.text()
    throw new Error(detail || `请求失败 (${response.status})`)
  }
  return response.json() as Promise<T>
}

export const api = {
  catalog: () => request<CatalogResponse>('/api/catalog'),
  setPlatemoPath: (platemoPath: string) => request<{ platemo_path: string }>('/api/platemo-path', {
    method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ platemo_path: platemoPath }),
  }),
  workers: () => request<Worker[]>('/api/workers'),
  tasks: () => request<TaskStatus[]>('/api/v1/ui/tasks'),
  schedulerStatus: () => request<{ paused: boolean }>('/api/v1/scheduler/status'),
  setSchedulerPaused: (paused: boolean) => request<{ paused: boolean }>('/api/v1/scheduler/pause', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ paused }),
  }),
  setWorkerDispatchPaused: (workerId: string, paused: boolean) => request<{ paused: boolean }>(
    `/api/v1/workers/${encodeURIComponent(workerId)}/dispatch-pause`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ paused }),
    },
  ),
  cancelAllTasks: () => request<{ status: string }>('/api/v1/seed-runs/cancel-all', { method: 'POST' }),
  deleteSeedHistory: (pointId: string, seed: number | string) => request<{ status: string }>(
    `/api/v1/seed-runs/${encodeURIComponent(pointId)}/${encodeURIComponent(String(seed))}/history`, { method: 'DELETE' },
  ),
  deleteSeedHistories: (runs: Array<{ pointId: string; seed: number | string }>) => request<{ status: string; count: number }>(
    '/api/v1/seed-runs/history/delete', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ runs: runs.map(({ pointId, seed }) => ({ experiment_point_id: pointId, seed: Number(seed) })) }),
    },
  ),
  cancelPoint: (pointId: string) => request<{ status: string }>(`/api/v1/experiment-points/${encodeURIComponent(pointId)}/cancel`, { method: 'POST' }),
  cancelBatch: (batchId: string) => request<{ status: string }>(`/api/v1/batch-attempts/${encodeURIComponent(batchId)}/cancel`, { method: 'POST' }),
  cancelSeedAttempt: (attemptId: string) => request<{ status: string }>(`/api/v2/seed-attempts/${encodeURIComponent(attemptId)}/cancel`, { method: 'POST' }),
  previewSetting: (filename: string) => request<ImportedSettings>(`/api/settings/preview?filename=${encodeURIComponent(filename)}`),
  importSetting: (file: File) => {
    const body = new FormData()
    body.append('settings_upload', file)
    return request<{ filename: string; values: ImportedSettings }>('/api/settings/load', { method: 'POST', body })
  },
  saveSetting: async (config: object, filename: string) => {
    const body = new FormData()
    body.append('config_json', JSON.stringify(config))
    body.append('filename', filename)
    const response = await fetch('/api/settings/save', { method: 'POST', body })
    if (!response.ok) throw new Error(await response.text())
    return response.blob()
  },
  probeWorkers: async () => {
    const response = await fetch('/api/workers/probe-all', { method: 'POST', redirect: 'follow' })
    if (!response.ok) throw new Error(await response.text())
  },
  addWorker: async (form: { name: string; url: string; token: string; priority: number }) => {
    const body = new FormData()
    Object.entries(form).forEach(([key, value]) => body.append(key, String(value)))
    const response = await fetch('/api/workers', { method: 'POST', body, redirect: 'follow' })
    if (!response.ok) throw new Error(await response.text())
  },
  editWorker: async (id: string, form: { name: string; url: string; token: string; priority: number }) => {
    const body = new FormData()
    Object.entries(form).forEach(([key, value]) => body.append(key, String(value)))
    const response = await fetch(`/api/workers/${id}/edit`, { method: 'POST', body, redirect: 'follow' })
    if (!response.ok) throw new Error(await response.text())
  },
  deleteWorker: async (id: string) => {
    const response = await fetch(`/api/workers/${id}/delete`, { method: 'POST', redirect: 'follow' })
    if (!response.ok) throw new Error(await response.text())
  },
  startRun: async (payload: {
    algorithms: object[]
    problems: object[]
    runs: number
    retainPoints: number
    clusterProfile?: string
    requiredPlatemoCommit?: string
    minimumDiskFreeBytes?: number
    settingsUpload?: File | null
  }) => {
    const body = new FormData()
    body.append('algorithms_json', JSON.stringify(payload.algorithms))
    body.append('problems_json', JSON.stringify(payload.problems))
    body.append('runs', String(payload.runs))
    body.append('retain_points', String(payload.retainPoints))
    if (payload.clusterProfile) body.append('cluster_profile', payload.clusterProfile)
    if (payload.requiredPlatemoCommit) body.append('required_platemo_commit', payload.requiredPlatemoCommit)
    if (payload.minimumDiskFreeBytes) body.append('minimum_disk_free_bytes', String(payload.minimumDiskFreeBytes))
    if (payload.settingsUpload) body.append('settings_upload', payload.settingsUpload)
    const response = await fetch('/api/v1/experiments', { method: 'POST', body })
    if (!response.ok) throw new Error(await response.text())
  },
}
