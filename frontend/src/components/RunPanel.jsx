/**
 * components/RunPanel.jsx — Trigger analysis runs
 * components/RunHistory.jsx — View run results and findings
 * components/ProfileViewer.jsx — Browse column statistics
 *
 * All in one file since they share state patterns.
 */

import { useState, useEffect, useCallback } from 'react'
import { PlayCircle, Loader, CheckCircle, AlertTriangle, ShieldAlert, Info,
         ChevronDown, ChevronUp, FileCode2, Database, BarChart3, RefreshCw } from 'lucide-react'
import toast from 'react-hot-toast'
import { listConnections, startRun, listRuns, getRun, getFindings, getProfiles, exportDbtSchema } from '../utils/api'

// ── Severity badge ──────────────────────────────────────────────────────────
const SeverityBadge = ({ severity }) => {
  const styles = {
    critical: 'bg-red-500/20 text-red-400 border-red-500/30',
    warning:  'bg-amber-500/20 text-amber-400 border-amber-500/30',
    info:     'bg-sky-500/20 text-sky-400 border-sky-500/30',
  }
  const icons = {
    critical: <ShieldAlert size={11} />,
    warning:  <AlertTriangle size={11} />,
    info:     <Info size={11} />
  }
  return (
    <span className={`badge border ${styles[severity] || styles.info}`}>
      {icons[severity]} {severity}
    </span>
  )
}

// ── Finding card ────────────────────────────────────────────────────────────
function FindingCard({ finding }) {
  const [expanded, setExpanded] = useState(finding.severity === 'critical')
  return (
    <div className={`border rounded-xl overflow-hidden transition-all ${
      finding.severity === 'critical' ? 'border-red-500/30 bg-red-500/5'
      : finding.severity === 'warning' ? 'border-amber-500/20'
      : 'border-gray-700/50'
    }`}>
      <button className="w-full flex items-center gap-3 p-4 text-left" onClick={() => setExpanded(!expanded)}>
        <SeverityBadge severity={finding.severity} />
        <div className="flex-1 min-w-0">
          <p className="text-sm font-medium text-white truncate">{finding.title}</p>
          <p className="text-xs text-gray-500 mt-0.5">
            {finding.table_name}{finding.column_name && ` → ${finding.column_name}`}
          </p>
        </div>
        {expanded ? <ChevronUp size={14} className="text-gray-500 shrink-0" /> : <ChevronDown size={14} className="text-gray-500 shrink-0" />}
      </button>
      {expanded && (
        <div className="px-4 pb-4 space-y-3 border-t border-gray-800">
          {finding.description && (
            <p className="text-sm text-gray-300 mt-3">{finding.description}</p>
          )}
          {finding.suggestion && (
            <div className="p-3 bg-gray-800 rounded-lg">
              <p className="text-xs text-gray-500 mb-1">💡 Suggestion</p>
              <p className="text-sm text-gray-200 font-mono text-xs whitespace-pre-wrap">{finding.suggestion}</p>
            </div>
          )}
          {finding.metric_value !== null && finding.metric_value !== undefined && (
            <p className="text-xs text-gray-600">
              Metric: {typeof finding.metric_value === 'number' ? finding.metric_value.toFixed(4) : finding.metric_value}
              {finding.threshold != null && ` (threshold: ${finding.threshold})`}
            </p>
          )}
        </div>
      )}
    </div>
  )
}

// ── Run Panel ───────────────────────────────────────────────────────────────
export function RunPanel({ onRunStarted }) {
  const [connections, setConnections] = useState([])
  const [selectedConn, setSelectedConn] = useState('')
  const [running, setRunning] = useState(false)

  useEffect(() => { listConnections().then(setConnections).catch(() => {}) }, [])

  const handleRun = async () => {
    if (!selectedConn) { toast.error('Select a connection first'); return }
    setRunning(true)
    try {
      const run = await startRun(selectedConn)
      toast.success('Analysis started! Redirecting to results...')
      setTimeout(() => onRunStarted(run.id), 1000)
    } catch (e) {
      toast.error(e.response?.data?.detail || 'Failed to start run')
      setRunning(false)
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-white">Run Analysis</h1>
        <p className="text-gray-400 text-sm mt-1">Select a database connection and run the AI data quality agent.</p>
      </div>

      <div className="card space-y-5">
        <div>
          <label className="label">Database Connection</label>
          <select className="input" value={selectedConn} onChange={e => setSelectedConn(e.target.value)}>
            <option value="">— Select a connection —</option>
            {connections.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
          </select>
          {connections.length === 0 && (
            <p className="text-xs text-amber-400 mt-1">No connections yet — add one in the Connections tab first</p>
          )}
        </div>

        <div className="p-4 bg-gray-800/50 rounded-xl space-y-2 text-sm text-gray-400">
          <p className="font-medium text-gray-200">What the agent does:</p>
          <div className="space-y-1.5 text-xs">
            <p>① <span className="text-gray-300">Profile</span> — runs SQL stats on every column (nulls, distributions, percentiles)</p>
            <p>② <span className="text-gray-300">Detect</span> — statistical anomaly detection (IQR outliers, null rates, casing inconsistencies)</p>
            <p>③ <span className="text-gray-300">Interpret</span> — Claude explains why each issue matters and how to fix it</p>
            <p>④ <span className="text-gray-300">Generate</span> — auto-creates dbt schema.yml with appropriate tests</p>
          </div>
        </div>

        <button onClick={handleRun} disabled={running || !selectedConn} className="btn-primary w-full justify-center py-3 text-base">
          {running ? <><Loader size={18} className="animate-spin" /> Starting analysis...</> : <><PlayCircle size={18} /> Run Data Quality Analysis</>}
        </button>
      </div>
    </div>
  )
}

// ── Run History ─────────────────────────────────────────────────────────────
export function RunHistory({ initialRunId }) {
  const [runs, setRuns] = useState([])
  const [selectedRun, setSelectedRun] = useState(null)
  const [findings, setFindings] = useState([])
  const [polling, setPolling] = useState(false)
  const [exportingDbt, setExportingDbt] = useState(false)
  const [severityFilter, setSeverityFilter] = useState(null)

  useEffect(() => { listRuns().then(setRuns).catch(() => {}) }, [])

  useEffect(() => {
    if (initialRunId) loadRun(initialRunId)
  }, [initialRunId])

  const loadRun = useCallback(async (runId) => {
    try {
      const run = await getRun(runId)
      setSelectedRun(run)
      if (run.status === 'running') {
        setPolling(true)
        const interval = setInterval(async () => {
          const updated = await getRun(runId)
          setSelectedRun(updated)
          if (updated.status !== 'running') {
            clearInterval(interval)
            setPolling(false)
            if (updated.status === 'done') {
              const f = await getFindings(runId)
              setFindings(f)
              toast.success(`Analysis complete — ${updated.issues_found} issues found`)
            }
          }
        }, 3000)
        return () => clearInterval(interval)
      } else if (run.status === 'done') {
        const f = await getFindings(runId)
        setFindings(f)
      }
    } catch { toast.error('Failed to load run') }
  }, [])

  const handleExportDbt = async () => {
    if (!selectedRun) return
    setExportingDbt(true)
    try {
      await exportDbtSchema(selectedRun.id)
      toast.success('schema.yml downloaded!')
    } catch { toast.error('Export failed') }
    finally { setExportingDbt(false) }
  }

  const filteredFindings = severityFilter
    ? findings.filter(f => f.severity === severityFilter)
    : findings

  const severityCounts = {
    critical: findings.filter(f => f.severity === 'critical').length,
    warning:  findings.filter(f => f.severity === 'warning').length,
    info:     findings.filter(f => f.severity === 'info').length,
  }

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold text-white">Run History</h1>

      <div className="flex gap-4">
        {/* Run list */}
        <div className="w-56 shrink-0 space-y-2">
          {runs.map(run => (
            <button key={run.id} onClick={() => loadRun(run.id)}
              className={`w-full text-left p-3 rounded-lg border transition-all ${
                selectedRun?.id === run.id
                  ? 'border-violet-500/50 bg-violet-500/10'
                  : 'border-gray-700 hover:border-gray-600 bg-gray-900'
              }`}>
              <div className="flex items-center gap-2 mb-1">
                {run.status === 'running' ? <Loader size={12} className="animate-spin text-violet-400" />
                  : run.status === 'done' ? <CheckCircle size={12} className="text-emerald-400" />
                  : <AlertTriangle size={12} className="text-red-400" />}
                <span className={`text-xs font-medium capitalize ${
                  run.status === 'done' ? 'text-emerald-400' : run.status === 'running' ? 'text-violet-400' : 'text-red-400'
                }`}>{run.status}</span>
              </div>
              <p className="text-xs text-gray-300">{run.tables_analyzed} tables</p>
              <p className="text-xs text-gray-500">{run.issues_found} issues</p>
              <p className="text-xs text-gray-600">{new Date(run.started_at).toLocaleDateString()}</p>
            </button>
          ))}
          {runs.length === 0 && <p className="text-xs text-gray-600">No runs yet</p>}
        </div>

        {/* Run detail */}
        <div className="flex-1 min-w-0 space-y-4">
          {!selectedRun && (
            <div className="card text-center py-12 text-gray-500">Select a run to view results</div>
          )}

          {selectedRun?.status === 'running' && (
            <div className="card flex flex-col items-center gap-3 py-12">
              <Loader size={32} className="text-violet-400 animate-spin" />
              <p className="text-gray-300 font-medium">Analysis in progress...</p>
              <p className="text-xs text-gray-500">Profiling columns → Detecting anomalies → Calling Claude...</p>
              <p className="text-xs text-gray-600">This typically takes 30–60 seconds</p>
            </div>
          )}

          {selectedRun?.status === 'done' && (
            <>
              {/* Summary */}
              <div className="card">
                <div className="flex items-center justify-between mb-3">
                  <h2 className="font-semibold text-white">Executive Summary</h2>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-gray-500">{selectedRun.duration_seconds?.toFixed(1)}s</span>
                    <button onClick={handleExportDbt} disabled={exportingDbt}
                      className="btn-primary py-1.5 text-xs">
                      {exportingDbt ? <Loader size={12} className="animate-spin" /> : <FileCode2 size={12} />}
                      Export dbt schema.yml
                    </button>
                  </div>
                </div>
                <p className="text-sm text-gray-300 leading-relaxed">{selectedRun.ai_summary}</p>
                <div className="flex gap-4 mt-4 pt-4 border-t border-gray-800">
                  <div className="text-center">
                    <p className="text-xl font-bold text-white">{selectedRun.tables_analyzed}</p>
                    <p className="text-xs text-gray-500">Tables</p>
                  </div>
                  <div className="text-center">
                    <p className="text-xl font-bold text-white">{selectedRun.columns_profiled}</p>
                    <p className="text-xs text-gray-500">Columns</p>
                  </div>
                  <div className="text-center">
                    <p className="text-xl font-bold text-red-400">{severityCounts.critical}</p>
                    <p className="text-xs text-gray-500">Critical</p>
                  </div>
                  <div className="text-center">
                    <p className="text-xl font-bold text-amber-400">{severityCounts.warning}</p>
                    <p className="text-xs text-gray-500">Warnings</p>
                  </div>
                </div>
              </div>

              {/* Filter + findings */}
              <div className="flex items-center gap-2">
                <span className="text-xs text-gray-500">Filter:</span>
                {[null, 'critical', 'warning', 'info'].map(sev => (
                  <button key={sev ?? 'all'} onClick={() => setSeverityFilter(sev)}
                    className={`text-xs px-3 py-1 rounded-full border transition-all ${
                      severityFilter === sev
                        ? 'border-violet-500 bg-violet-500/20 text-violet-300'
                        : 'border-gray-700 text-gray-500 hover:border-gray-600'
                    }`}>
                    {sev ? `${sev} (${severityCounts[sev]})` : `All (${findings.length})`}
                  </button>
                ))}
              </div>

              <div className="space-y-2">
                {filteredFindings.map(f => <FindingCard key={f.id} finding={f} />)}
                {filteredFindings.length === 0 && (
                  <p className="text-center text-gray-600 text-sm py-8">No findings for this filter.</p>
                )}
              </div>
            </>
          )}

          {selectedRun?.status === 'error' && (
            <div className="card border-red-500/30 bg-red-500/5">
              <p className="text-red-400 font-medium">Analysis failed</p>
              <p className="text-sm text-gray-400 mt-1">{selectedRun.error_message}</p>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

// ── Profile Viewer ──────────────────────────────────────────────────────────
export function ProfileViewer() {
  const [runs, setRuns] = useState([])
  const [selectedRun, setSelectedRun] = useState('')
  const [profiles, setProfiles] = useState([])
  const [tables, setTables] = useState([])
  const [selectedTable, setSelectedTable] = useState('')

  useEffect(() => {
    listRuns().then(r => setRuns(r.filter(x => x.status === 'done'))).catch(() => {})
  }, [])

  const loadProfiles = async (runId) => {
    setSelectedRun(runId)
    const data = await getProfiles(runId)
    setProfiles(data)
    setTables([...new Set(data.map(p => p.table_name))])
    setSelectedTable('')
  }

  const filtered = selectedTable ? profiles.filter(p => p.table_name === selectedTable) : profiles

  return (
    <div className="space-y-5">
      <div>
        <h1 className="text-2xl font-bold text-white">Column Profiles</h1>
        <p className="text-gray-400 text-sm mt-1">Browse detailed statistics for every profiled column.</p>
      </div>

      <div className="flex gap-3">
        <div className="flex-1">
          <label className="label">Select Run</label>
          <select className="input" value={selectedRun} onChange={e => loadProfiles(e.target.value)}>
            <option value="">— Select a completed run —</option>
            {runs.map(r => <option key={r.id} value={r.id}>{new Date(r.started_at).toLocaleDateString()} — {r.tables_analyzed} tables</option>)}
          </select>
        </div>
        {tables.length > 0 && (
          <div className="flex-1">
            <label className="label">Filter Table</label>
            <select className="input" value={selectedTable} onChange={e => setSelectedTable(e.target.value)}>
              <option value="">All tables</option>
              {tables.map(t => <option key={t}>{t}</option>)}
            </select>
          </div>
        )}
      </div>

      {filtered.length > 0 && (
        <div className="space-y-2">
          {filtered.map(p => (
            <div key={p.id} className="card text-sm">
              <div className="flex items-center justify-between mb-3">
                <div>
                  <span className="font-medium text-white">{p.table_name}</span>
                  <span className="text-gray-500 mx-1">→</span>
                  <span className="font-mono text-violet-300">{p.column_name}</span>
                </div>
                <span className="badge bg-gray-800 text-gray-400 text-xs">{p.data_type}</span>
              </div>
              <div className="grid grid-cols-4 gap-3 text-xs">
                <div>
                  <p className="text-gray-500">Rows</p>
                  <p className="text-white font-medium">{p.row_count?.toLocaleString()}</p>
                </div>
                <div>
                  <p className="text-gray-500">Null Rate</p>
                  <p className={`font-medium ${(p.null_rate || 0) > 0.05 ? 'text-amber-400' : 'text-white'}`}>
                    {((p.null_rate || 0) * 100).toFixed(1)}%
                  </p>
                </div>
                <div>
                  <p className="text-gray-500">Unique</p>
                  <p className="text-white font-medium">{p.unique_count?.toLocaleString()}</p>
                </div>
                <div>
                  <p className="text-gray-500">Unique Rate</p>
                  <p className="text-white font-medium">{((p.unique_rate || 0) * 100).toFixed(1)}%</p>
                </div>
                {p.min_value !== null && p.min_value !== undefined && (
                  <>
                    <div><p className="text-gray-500">Min</p><p className={`font-medium ${p.min_value < 0 ? 'text-red-400' : 'text-white'}`}>{p.min_value?.toFixed(2)}</p></div>
                    <div><p className="text-gray-500">Max</p><p className="text-white font-medium">{p.max_value?.toFixed(2)}</p></div>
                    <div><p className="text-gray-500">Mean</p><p className="text-white font-medium">{p.mean_value?.toFixed(2)}</p></div>
                    <div><p className="text-gray-500">p99</p><p className="text-white font-medium">{p.p99?.toFixed(2)}</p></div>
                  </>
                )}
                {p.top_values && Object.keys(p.top_values).length > 0 && (
                  <div className="col-span-4">
                    <p className="text-gray-500 mb-1">Top Values</p>
                    <div className="flex flex-wrap gap-1">
                      {Object.entries(p.top_values).slice(0, 8).map(([val, cnt]) => (
                        <span key={val} className="badge bg-gray-800 text-gray-300 border border-gray-700">
                          {val} <span className="text-gray-600">({cnt})</span>
                        </span>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>
          ))}
        </div>
      )}

      {selectedRun && filtered.length === 0 && (
        <p className="text-center text-gray-600 text-sm">No profiles found.</p>
      )}
    </div>
  )
}

export default RunHistory
