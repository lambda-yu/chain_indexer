import { useQuery } from '@tanstack/react-query'
import { api } from '@/api/client'
import { Activity, Box, Radio, Link2, Upload, CheckCircle, XCircle, AlertTriangle } from 'lucide-react'

interface Health { db: string; redis: string }
interface Chain { id: string; kind: string; enabled: boolean }
interface ChainStatus { chain_id: string; enabled: boolean; latest_block: number | null; latest_block_hash: string | null }
interface FailedDelivery { id: string; status: string }

export default function Dashboard() {
  const { data: health } = useQuery<Health>({ queryKey: ['health'], queryFn: () => api.get('/healthz'), refetchInterval: 10000 })
  const { data: chains = [] } = useQuery<Chain[]>({ queryKey: ['chains'], queryFn: () => api.get('/chains') })
  const { data: channels = [] } = useQuery<unknown[]>({ queryKey: ['channels'], queryFn: () => api.get('/channels') })
  const { data: subs = [] } = useQuery<unknown[]>({ queryKey: ['subscriptions'], queryFn: () => api.get('/subscriptions') })
  const { data: abis = [] } = useQuery<unknown[]>({ queryKey: ['abis'], queryFn: () => api.get('/abis') })
  const { data: failures = [] } = useQuery<FailedDelivery[]>({ queryKey: ['failed-deliveries'], queryFn: () => api.get('/failed-deliveries'), refetchInterval: 10000 })

  const ok = (s: string) => s === 'ok'
  const failedCount = failures.filter(f => f.status === 'failed').length

  return (
    <div>
      <h2 className="text-2xl font-bold mb-6">仪表盘</h2>

      <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-8">
        <StatCard icon={Box} label="链" value={chains.length} />
        <StatCard icon={Radio} label="渠道" value={channels.length} />
        <StatCard icon={Link2} label="订阅" value={subs.length} />
        <StatCard icon={Upload} label="ABI" value={abis.length} />
        <div className={`border rounded-lg p-4 flex items-center gap-3 ${failedCount > 0 ? 'border-red-300 bg-red-50' : ''}`}>
          <AlertTriangle size={20} className={failedCount > 0 ? 'text-red-500' : 'text-gray-400'} />
          <div><p className={`text-2xl font-bold ${failedCount > 0 ? 'text-red-600' : ''}`}>{failedCount}</p><p className="text-xs text-gray-500">失败投递</p></div>
        </div>
      </div>

      <h3 className="text-lg font-semibold mb-3">系统健康</h3>
      {health ? (
        <div className="flex gap-4 mb-8">
          <HealthBadge label="数据库" ok={ok(health.db)} />
          <HealthBadge label="Redis" ok={ok(health.redis)} />
        </div>
      ) : <p className="text-gray-400 text-sm mb-8">检查中...</p>}

      <h3 className="text-lg font-semibold mb-3">链状态</h3>
      {chains.length === 0 ? <p className="text-gray-400 text-sm">暂无配置链。</p> : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
          {chains.map(c => <ChainCard key={c.id} chain={c} />)}
        </div>
      )}
    </div>
  )
}

function ChainCard({ chain }: { chain: Chain }) {
  const { data: status } = useQuery<ChainStatus>({
    queryKey: ['chain-status', chain.id],
    queryFn: () => api.get(`/chains/${chain.id}/status`),
    refetchInterval: 5000,
    enabled: chain.enabled,
  })
  return (
    <div className={`border rounded-lg p-4 ${chain.enabled ? '' : 'opacity-50'}`}>
      <div className="flex items-center justify-between mb-2">
        <span className="font-mono text-sm font-medium">{chain.id}</span>
        <span className={`px-2 py-0.5 rounded text-xs ${chain.kind === 'evm' ? 'bg-blue-100 text-blue-700' : 'bg-purple-100 text-purple-700'}`}>{chain.kind}</span>
      </div>
      <div className="flex items-center gap-1 text-xs mb-2">
        {chain.enabled ? <><Activity size={12} className="text-green-600" /><span className="text-green-600">运行中</span></> : <span className="text-gray-400">已停用</span>}
      </div>
      {status && status.latest_block !== null ? (
        <div className="bg-gray-50 rounded p-2 text-xs space-y-1">
          <div className="flex justify-between"><span className="text-gray-500">最新区块</span><span className="font-mono font-medium">{status.latest_block.toLocaleString()}</span></div>
          <div className="flex justify-between"><span className="text-gray-500">区块哈希</span><span className="font-mono truncate max-w-32">{status.latest_block_hash?.slice(0, 18)}...</span></div>
        </div>
      ) : chain.enabled ? (
        <p className="text-xs text-gray-400">等待同步...</p>
      ) : null}
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
      {ok ? <CheckCircle size={16} /> : <XCircle size={16} />} {label}: {ok ? '正常' : '异常'}
    </div>
  )
}
