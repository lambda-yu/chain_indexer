import { useQuery } from '@tanstack/react-query'
import { api } from '@/api/client'
import { Activity, Box, Radio, Link2, Upload, CheckCircle, XCircle } from 'lucide-react'

interface Health { db: string; redis: string }

export default function Dashboard() {
  const { data: health } = useQuery<Health>({ queryKey: ['health'], queryFn: () => api.get('/healthz'), refetchInterval: 10000 })
  const { data: chains = [] } = useQuery<{ id: string; kind: string; enabled: boolean }[]>({ queryKey: ['chains'], queryFn: () => api.get('/chains') })
  const { data: channels = [] } = useQuery<unknown[]>({ queryKey: ['channels'], queryFn: () => api.get('/channels') })
  const { data: subs = [] } = useQuery<unknown[]>({ queryKey: ['subscriptions'], queryFn: () => api.get('/subscriptions') })
  const { data: abis = [] } = useQuery<unknown[]>({ queryKey: ['abis'], queryFn: () => api.get('/abis') })

  const ok = (s: string) => s === 'ok'

  return (
    <div>
      <h2 className="text-2xl font-bold mb-6">Dashboard</h2>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
        <StatCard icon={Box} label="Chains" value={chains.length} />
        <StatCard icon={Radio} label="Channels" value={channels.length} />
        <StatCard icon={Link2} label="Subscriptions" value={subs.length} />
        <StatCard icon={Upload} label="ABIs" value={abis.length} />
      </div>

      <h3 className="text-lg font-semibold mb-3">System Health</h3>
      {health ? (
        <div className="flex gap-4 mb-8">
          <HealthBadge label="Database" ok={ok(health.db)} />
          <HealthBadge label="Redis" ok={ok(health.redis)} />
        </div>
      ) : <p className="text-gray-400 text-sm mb-8">Checking...</p>}

      <h3 className="text-lg font-semibold mb-3">Active Chains</h3>
      {chains.length === 0 ? <p className="text-gray-400 text-sm">No chains configured.</p> : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
          {chains.filter(c => c.enabled).map(c => (
            <div key={c.id} className="border rounded-lg p-4">
              <div className="flex items-center justify-between">
                <span className="font-mono text-sm font-medium">{c.id}</span>
                <span className={`px-2 py-0.5 rounded text-xs ${c.kind === 'evm' ? 'bg-blue-100 text-blue-700' : 'bg-purple-100 text-purple-700'}`}>{c.kind}</span>
              </div>
              <div className="flex items-center gap-1 mt-2 text-green-600 text-xs"><Activity size={12} /> Running</div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

function StatCard({ icon: Icon, label, value }: { icon: typeof Box; label: string; value: number }) {
  return (
    <div className="border rounded-lg p-4 flex items-center gap-3">
      <Icon size={20} className="text-gray-400" />
      <div><p className="text-2xl font-bold">{value}</p><p className="text-xs text-gray-500">{label}</p></div>
    </div>
  )
}

function HealthBadge({ label, ok }: { label: string; ok: boolean }) {
  return (
    <div className={`flex items-center gap-2 px-3 py-2 rounded-lg text-sm ${ok ? 'bg-green-50 text-green-700' : 'bg-red-50 text-red-700'}`}>
      {ok ? <CheckCircle size={16} /> : <XCircle size={16} />} {label}: {ok ? 'OK' : 'Fail'}
    </div>
  )
}
