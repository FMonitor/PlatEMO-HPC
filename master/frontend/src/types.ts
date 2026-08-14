export interface ParameterSchema {
  name: string
  default: string
  description?: string
  source?: string
}

export interface CatalogItem {
  name: string
  kind: string
  path?: string
  relative_path?: string
  parameters: ParameterSchema[]
}

export interface ExperimentItem {
  id: string
  name: string
  parameters: Record<string, string | number | boolean | string[]>
}

export interface SettingFile {
  name: string
  filename: string
  path: string
  size: number
  modified_at: number
}

export interface ExistingTest {
  algorithm: string
  problem: string
  M: number
  D: number
  runs: number[]
  run_count: number
  file_count: number
  files: string[]
}

export interface Worker {
  id: string
  name: string
  url: string
  online: number
  queue_count: number | null
  last_check: string
  health_error: string
  priority?: number
  dispatch_paused?: number
  configured_pool_workers?: number
  max_seeds_per_batch?: number
  profile_ready?: boolean
  status?: string
  last_heartbeat?: string
  active_batch?: {
    id: string
    state: string
    assigned_at: string
    accepted_at: string
    lease_deadline: string
    matlab_pid: number
    pool: Record<string, unknown>
  } | null
  dynamic_session?: {
    id: string
    state: string
    configured_workers: number
    actual_workers: number
    free_seed_slots: number
    running_seeds: number
    last_heartbeat: string
  } | null
}

export interface TaskStatus {
  id: string
  task_id?: string
  state: string
  worker_id: string | null
  worker_name: string
  algorithm: string
  problem: string
  seed: number | string
  parameters: Record<string, string | number | boolean | string[]>
  algorithm_parameters?: Record<string, string | number | boolean | string[]>
  created_at: string
  updated_at: string
  error: string
  attempt_kind?: 'batch' | 'dynamic_seed'
  fe?: number
  total_fe?: number
  elapsed_seconds?: number
  batch_attempt_id?: string
}

export interface ImportedSettings {
  format: 'platemo-setting' | 'platemo-hpc-settings'
  source: string
  algorithms: ExperimentItem[]
  problems: ExperimentItem[]
  runs: number
  retain_results: number
  diagnostics: string[]
}

export interface CatalogResponse {
  algorithms: CatalogItem[]
  problems: CatalogItem[]
  settings: SettingFile[]
  existing_tests: ExistingTest[]
  platemo_path: string
}
