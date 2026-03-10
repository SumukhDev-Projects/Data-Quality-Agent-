import { useState, useEffect } from 'react'
import { Plus, Database, Trash2, CheckCircle, Loader } from 'lucide-react'
import toast from 'react-hot-toast'
import { createConnection, listConnections, deleteConnection } from '../utils/api'

export default function Connections() {
  const [connections, setConnections] = useState([])
  const [creating, setCreating] = useState(false)
  const [saving, setSaving] = useState(false)
  const [form, setForm] = useState({ name: '', description: '', conn_string: '' })

  const load = () => listConnections().then(setConnections).catch(() => {})
  useEffect(() => { load() }, [])

  const handleSave = async () => {
    if (!form.name || !form.conn_string) { toast.error('Name and connection string required'); return }
    setSaving(true)
    try {
      await createConnection(form)
      toast.success('Connection saved and tested!')
      setCreating(false)
      setForm({ name: '', description: '', conn_string: '' })
      await load()
    } catch (e) {
      toast.error(e.response?.data?.detail || 'Connection failed')
    } finally { setSaving(false) }
  }

  const handleDelete = async (id, name) => {
    if (!confirm(`Delete "${name}"?`)) return
    await deleteConnection(id)
    setConnections(prev => prev.filter(c => c.id !== id))
    toast.success('Deleted')
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Database Connections</h1>
          <p className="text-gray-400 text-sm mt-1">Add PostgreSQL databases to analyze.</p>
        </div>
        {!creating && (
          <button onClick={() => setCreating(true)} className="btn-primary">
            <Plus size={16} /> Add Connection
          </button>
        )}
      </div>

      {/* Sample DB helper */}
      <div className="card border-violet-500/20 bg-violet-500/5">
        <p className="text-sm font-medium text-violet-300 mb-2">🚀 Try it with the included sample database</p>
        <p className="text-xs text-gray-400 mb-3">
          The sample e-commerce database includes intentional data quality issues for you to discover.
        </p>
        <code className="text-xs bg-gray-800 px-3 py-2 rounded block text-violet-300">
          postgresql://sample:sample_secret@sample_db:5432/ecommerce
        </code>
        <button onClick={() => { setForm({ name: 'Sample E-commerce DB', description: 'Included demo database with intentional DQ issues', conn_string: 'postgresql://sample:sample_secret@sample_db:5432/ecommerce' }); setCreating(true) }}
          className="mt-3 text-xs text-violet-400 hover:text-violet-300 underline">
          Pre-fill form with sample DB →
        </button>
      </div>

      {/* Create form */}
      {creating && (
        <div className="card space-y-4">
          <h2 className="font-semibold text-white">New Connection</h2>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="label">Name *</label>
              <input className="input" placeholder="e.g. Production DB"
                value={form.name} onChange={e => setForm(p => ({ ...p, name: e.target.value }))} />
            </div>
            <div>
              <label className="label">Description</label>
              <input className="input" placeholder="What is this database?"
                value={form.description} onChange={e => setForm(p => ({ ...p, description: e.target.value }))} />
            </div>
          </div>
          <div>
            <label className="label">Connection String * (PostgreSQL only)</label>
            <input className="input font-mono text-xs" placeholder="postgresql://user:password@host:5432/dbname"
              value={form.conn_string} onChange={e => setForm(p => ({ ...p, conn_string: e.target.value }))} />
            <p className="text-xs text-gray-600 mt-1">Connection is tested before saving.</p>
          </div>
          <div className="flex gap-3">
            <button onClick={handleSave} disabled={saving} className="btn-primary">
              {saving ? <><Loader size={14} className="animate-spin" /> Testing & Saving...</> : <><CheckCircle size={14} /> Save Connection</>}
            </button>
            <button onClick={() => setCreating(false)} className="btn-ghost">Cancel</button>
          </div>
        </div>
      )}

      {/* Connection list */}
      {connections.map(c => (
        <div key={c.id} className="card flex items-center gap-4">
          <div className="w-10 h-10 bg-violet-500/20 rounded-lg flex items-center justify-center shrink-0">
            <Database size={18} className="text-violet-400" />
          </div>
          <div className="flex-1 min-w-0">
            <p className="font-medium text-white">{c.name}</p>
            {c.description && <p className="text-xs text-gray-500">{c.description}</p>}
            <p className="text-xs text-gray-600 mt-0.5">
              {c.last_run_at ? `Last analyzed: ${new Date(c.last_run_at).toLocaleDateString()}` : 'Never analyzed'}
            </p>
          </div>
          <span className="badge bg-emerald-500/20 text-emerald-400">Connected</span>
          <button onClick={() => handleDelete(c.id, c.name)} className="text-gray-600 hover:text-red-400 transition-colors p-1">
            <Trash2 size={14} />
          </button>
        </div>
      ))}

      {connections.length === 0 && !creating && (
        <p className="text-center text-gray-600 text-sm">No connections yet.</p>
      )}
    </div>
  )
}
