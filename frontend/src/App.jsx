import { useState } from 'react'
import { Toaster } from 'react-hot-toast'
import { LayoutDashboard, Database, PlayCircle, BarChart3, FileCode2, Search } from 'lucide-react'
import Dashboard from './components/Dashboard'
import Connections from './components/Connections'
import { RunPanel, RunHistory, ProfileViewer } from './components/RunPanel'

const TABS = [
  { id: 'dashboard',   label: 'Dashboard',    icon: LayoutDashboard },
  { id: 'connections', label: 'Connections',   icon: Database },
  { id: 'run',         label: 'Run Analysis',  icon: PlayCircle },
  { id: 'history',     label: 'Run History',   icon: BarChart3 },
  { id: 'profiles',    label: 'Column Profiles', icon: Search },
]

export default function App() {
  const [tab, setTab] = useState('dashboard')
  const [selectedRunId, setSelectedRunId] = useState(null)

  const goToRun = (runId) => { setSelectedRunId(runId); setTab('history') }

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100">
      <Toaster position="top-right" toastOptions={{
        style: { background: '#1f2937', color: '#f3f4f6', border: '1px solid #374151' }
      }} />

      <header className="border-b border-gray-800 bg-gray-900/80 backdrop-blur sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 h-16 flex items-center gap-3">
          <div className="flex items-center gap-2">
            <div className="w-8 h-8 bg-violet-500 rounded-lg flex items-center justify-center">
              <Search size={16} className="text-white" />
            </div>
            <span className="font-bold text-lg text-white">DQA</span>
            <span className="text-gray-500 text-sm hidden sm:block">/ AI Data Quality Agent</span>
          </div>
          <div className="ml-auto flex items-center gap-1 text-xs text-gray-500 bg-gray-800 px-3 py-1 rounded-full">
            <span className="w-2 h-2 bg-green-400 rounded-full inline-block"></span>
            Claude 3.5 Sonnet
          </div>
        </div>
      </header>

      <div className="max-w-7xl mx-auto px-4 py-6 flex gap-6">
        <nav className="w-48 shrink-0">
          <ul className="space-y-1">
            {TABS.map(({ id, label, icon: Icon }) => (
              <li key={id}>
                <button onClick={() => setTab(id)}
                  className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-all ${
                    tab === id
                      ? 'bg-violet-500/20 text-violet-400 border border-violet-500/30'
                      : 'text-gray-400 hover:text-gray-200 hover:bg-gray-800'
                  }`}>
                  <Icon size={16} />{label}
                </button>
              </li>
            ))}
          </ul>
        </nav>

        <main className="flex-1 min-w-0">
          {tab === 'dashboard'   && <Dashboard onNavigate={setTab} onOpenRun={goToRun} />}
          {tab === 'connections' && <Connections />}
          {tab === 'run'         && <RunPanel onRunStarted={goToRun} />}
          {tab === 'history'     && <RunHistory initialRunId={selectedRunId} />}
          {tab === 'profiles'    && <ProfileViewer />}
        </main>
      </div>
    </div>
  )
}
