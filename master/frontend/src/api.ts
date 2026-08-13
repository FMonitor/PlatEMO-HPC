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
  tasks: () => request<TaskStatus[]>('/api/tasks'),
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
  addWorker: async (form: { name: string; url: string; token: string }) => {
    const body = new FormData()
    Object.entries(form).forEach(([key, value]) => body.append(key, value))
    const response = await fetch('/api/workers', { method: 'POST', body, redirect: 'follow' })
    if (!response.ok) throw new Error(await response.text())
  },
  editWorker: async (id: string, form: { name: string; url: string; token: string }) => {
    const body = new FormData()
    Object.entries(form).forEach(([key, value]) => body.append(key, value))
    const response = await fetch(`/api/workers/${id}/edit`, { method: 'POST', body, redirect: 'follow' })
    if (!response.ok) throw new Error(await response.text())
  },
  deleteWorker: async (id: string) => {
    const response = await fetch(`/api/workers/${id}/delete`, { method: 'POST', redirect: 'follow' })
    if (!response.ok) throw new Error(await response.text())
  },
  startMock: async (payload: {
    algorithms: object[]
    problems: object[]
    runs: number
    maxWorkers: number | null
    retainPoints: number
    workerIds: string[]
  }) => {
    const body = new FormData()
    body.append('algorithms_json', JSON.stringify(payload.algorithms))
    body.append('problems_json', JSON.stringify(payload.problems))
    body.append('runs', String(payload.runs))
    body.append('retain_points', String(payload.retainPoints))
    if (payload.maxWorkers) body.append('max_workers', String(payload.maxWorkers))
    payload.workerIds.forEach((id) => body.append('worker_ids', id))
    const response = await fetch('/api/mock-runs', { method: 'POST', body, redirect: 'follow' })
    if (!response.ok) throw new Error(await response.text())
  },
}
