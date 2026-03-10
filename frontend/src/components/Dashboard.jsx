import { useEffect, useState } from 'react'
import { AlertTriangle, Database, PlayCircle, TrendingUp, ArrowRight, ShieldAlert } from 'lucide-react'
import { getStats } from '../utils/api'

const Card = ({ icon: Icon, label, value, color, sub }) => (
  <div className="card flex items-start gap-4">
    <div className={`w-10 h-10 rounded-lg flex items-center justify-center ${color}`}>
      <Icon size={18} />
    </div>
    <div>
      <p className="text-2xl font-bold text-white">{value ?? '—'}</p>
      <p className="text-sm text-gray-400">{label}</p>
      {sub && <p className="text-xs text-red-400 mt-0.5">{sub}</p>}
    </div>
  </div>
)

export default function Dashboard({ onNavigate }) {
  const [stats, setStats] = useState(null)

  useEffect(() => {
    getStats().then(setStats).catch(() => {})
    const t = setInterval(() => getStats().then(setStats).catch(() => {}), 30000)
    return () => clearInterval(t)
  }, [])

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-white">Dashboard</h1>
        <p className="text-gray-400 text-sm mt-1">
          AI agent that profiles any PostgreSQL database, detects anomalies, and auto-generates dbt tests.
        </p>
      </div>

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <Card icon={Database}     label="Connections"       value={stats?.total_connections}  color="bg-violet-500/20 text-violet-400" />
        <Card icon={PlayCircle}   label="Runs Completed"    value={stats?.total_runs}          color="bg-sky-500/20 text-sky-400" />
        <Card icon={TrendingUp}   label="Issues Found"      value={stats?.total_findings}      color="bg-amber-500/20 text-amber-400" />
        <Card icon={ShieldAlert}  label="Critical Issues"   value={stats?.critical_findings}
          color="bg-red-500/20 text-red-400"
          sub={stats?.critical_findings > 0 ? "needs attention" : undefined} />
      </div>

      <div className="card">
        <h2 className="font-semibold text-white mb-4">How It Works</h2>
        <div className="grid grid-cols-1 sm:grid-cols-4 gap-3">
          {[
            { step: '1', label: 'Connect', desc: 'Add your DB connection string', action: 'connections' },
            { step: '2', label: 'Profile', desc: 'Agent runs SQL stats on every column', action: 'run' },
            { step: '3', label: 'Detect', desc: 'Statistical + AI anomaly detection', action: 'run' },
            { step: '4', label: 'Export', desc: 'Download dbt schema.yml with tests', action: 'history' },
          ].map(({ step, label, desc, action }) => (
            <button key={step} onClick={() => onNavigate(action)}
              className="flex items-start gap-3 p-4 bg-gray-800/50 hover:bg-gray-800 border border-gray-700 hover:border-violet-500/50 rounded-xl text-left transition-all group">
              <span className="w-7 h-7 rounded-full bg-violet-500/20 text-violet-400 text-xs font-bold flex items-center justify-center shrink-0">
                {step}
              </span>
              <div>
                <p className="font-medium text-white text-sm">{label}</p>
                <p className="text-xs text-gray-500 mt-0.5">{desc}</p>
              </div>
            </button>
          ))}
        </div>
      </div>

      {stats?.critical_findings > 0 && (
        <button onClick={() => onNavigate('history')}
          className="w-full flex items-center gap-3 p-4 bg-red-500/10 border border-red-500/30 rounded-xl hover:border-red-500/60 transition-all text-left">
          <AlertTriangle size={18} className="text-red-400 shrink-0" />
          <div>
            <p className="text-sm font-medium text-red-300">{stats.critical_findings} critical issue{stats.critical_findings > 1 ? 's' : ''} detected</p>
            <p className="text-xs text-gray-500">Click to view latest run findings</p>
          </div>
          <ArrowRight size={14} className="ml-auto text-red-500" />
        </button>
      )}
    </div>
  )
}
