/**
 * utils/api.js — All backend API calls in one file.
 * Every component imports from here. Never calls axios directly.
 */
import axios from 'axios'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000'
const api = axios.create({ baseURL: API_URL, timeout: 120000 })

// ── Connections ────────────────────────────────────────────────────────────
export const createConnection = async (data) => (await api.post('/connections', data)).data
export const listConnections  = async ()     => (await api.get('/connections')).data
export const deleteConnection = async (id)   => (await api.delete(`/connections/${id}`)).data

// ── Runs ───────────────────────────────────────────────────────────────────
export const startRun    = async (connectionId, tables = null) =>
  (await api.post('/runs', { connection_id: connectionId, tables })).data
export const listRuns    = async ()    => (await api.get('/runs')).data
export const getRun      = async (id)  => (await api.get(`/runs/${id}`)).data
export const getProfiles = async (id, table = null) => {
  const params = table ? { table } : {}
  return (await api.get(`/runs/${id}/profiles`, { params })).data
}
export const getFindings = async (id, severity = null) => {
  const params = severity ? { severity } : {}
  return (await api.get(`/runs/${id}/findings`, { params })).data
}

// ── Export ─────────────────────────────────────────────────────────────────
export const exportDbtSchema = async (runId, modelPrefix = '') => {
  const response = await api.post('/export/dbt',
    { run_id: runId, model_prefix: modelPrefix },
    { responseType: 'blob' }
  )
  const url = window.URL.createObjectURL(new Blob([response.data]))
  const link = document.createElement('a')
  link.href = url
  link.setAttribute('download', 'schema.yml')
  document.body.appendChild(link)
  link.click()
  link.remove()
}

// ── Stats ──────────────────────────────────────────────────────────────────
export const getStats = async () => (await api.get('/stats')).data
