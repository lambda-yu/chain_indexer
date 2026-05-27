import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '@/api/client'
import { Play, Loader2 } from 'lucide-react'

interface Chain { id: string; kind: string }
interface MatchedSub { subscription_id: string; subscription_name: string; match_kind: string; match_name: string | null; channels: { id: string; name: string; type: string }[] }
interface ParseResult {
  chain_id: string; kind: string; block_number: number
  tx_count?: number; log_count?: number; event_count?: number; matched_count?: number
  events: (Record<string, unknown> & { matched?: boolean; matched_subscriptions?: MatchedSub[] })[]; error?: string
}

export default function BlockTest() {
  const { data: chains = [] } = useQuery<Chain[]>({ queryKey: ['chains'], queryFn: () => api.get('/chains') })
  const [chainId, setChainId] = useState('')
  const [blockNum, setBlockNum] = useState('')
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState<ParseResult | null>(null)
  const [error, setError] = useState('')

  const run = async () => {
    if (!chainId || !blockNum) return
    setLoading(true); setError(''); setResult(null)
    try {
      const res = await api.post<ParseResult>('/test/parse-block', { chain_id: chainId, block_number: Number(blockNum) })
      setResult(res)
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      if (msg.includes('502')) setError('RPC 连接失败，请检查链配置中的 RPC 地址是否正确且可访问。')
      else if (msg.includes('404')) setError('链不存在，请先在"链配置"页面添加。')
      else setError(msg)
    } finally { setLoading(false) }
  }

  return (
    <div>
      <h2 className="text-2xl font-bold mb-4">区块解析测试</h2>
      <p className="text-gray-500 text-sm mb-4">选择链并输入区块号，系统会拉取该区块数据并运行所有 parser，展示解析出的事件。</p>

      <div className="flex items-end gap-3 mb-6">
        <div>
          <label className="text-xs text-gray-500 block mb-1">链</label>
          <select value={chainId} onChange={e => setChainId(e.target.value)} className="border rounded px-3 py-1.5 text-sm w-48">
            <option value="">选择链...</option>
            {chains.map(c => <option key={c.id} value={c.id}>{c.id} ({c.kind})</option>)}
          </select>
        </div>
        <div>
          <label className="text-xs text-gray-500 block mb-1">区块号 / Slot</label>
          <input value={blockNum} onChange={e => setBlockNum(e.target.value)} type="number" placeholder="如 20000000" className="border rounded px-3 py-1.5 text-sm w-40" />
        </div>
        <button onClick={run} disabled={loading || !chainId || !blockNum} className="flex items-center gap-1 bg-black text-white px-4 py-1.5 rounded text-sm disabled:opacity-40">
          {loading ? <Loader2 size={14} className="animate-spin" /> : <Play size={14} />}
          {loading ? '解析中...' : '解析'}
        </button>
      </div>

      {error && <div className="bg-red-50 border border-red-200 rounded p-3 text-red-700 text-sm mb-4">{error}</div>}

      {result && (
        <div>
          <div className="flex gap-4 mb-4">
            <div className="border rounded-lg p-3 text-center">
              <p className="text-2xl font-bold">{result.event_count ?? result.events.length}</p>
              <p className="text-xs text-gray-500">解析事件</p>
            </div>
            <div className={`border rounded-lg p-3 text-center ${result.matched_count ? 'border-green-300 bg-green-50' : ''}`}>
              <p className="text-2xl font-bold text-green-600">{result.matched_count ?? 0}</p>
              <p className="text-xs text-gray-500">命中订阅</p>
            </div>
            {result.tx_count !== undefined && <div className="border rounded-lg p-3 text-center">
              <p className="text-2xl font-bold">{result.tx_count}</p>
              <p className="text-xs text-gray-500">交易数</p>
            </div>}
            {result.log_count !== undefined && <div className="border rounded-lg p-3 text-center">
              <p className="text-2xl font-bold">{result.log_count}</p>
              <p className="text-xs text-gray-500">日志数</p>
            </div>}
          </div>

          {result.error && <div className="bg-yellow-50 border border-yellow-200 rounded p-3 text-yellow-700 text-sm mb-4">{result.error}</div>}

          {result.events.length === 0 ? (
            <p className="text-gray-400 text-sm">该区块未解析出任何事件。可能没有匹配的交易，或需要上传对应的 ABI。</p>
          ) : (
            <div className="space-y-2">
              {result.events.map((ev, i) => (
                <div key={i} className={`border rounded-lg p-3 ${ev.matched ? 'border-green-300 bg-green-50/30' : ''}`}>
                  <div className="flex items-center gap-2 mb-2">
                    <span className="px-2 py-0.5 rounded text-xs font-medium bg-gray-100">{String(ev.kind)}</span>
                    {ev.name ? <span className="text-sm font-medium">{String(ev.name)}</span> : null}
                    {ev.matched ? <span className="px-1.5 py-0.5 rounded text-[10px] bg-green-100 text-green-700 font-medium">命中</span> : null}
                    {ev.contract ? <span className="text-xs font-mono text-gray-400 truncate max-w-64 ml-auto">{String(ev.contract)}</span> : null}
                  </div>
                  {ev.args && typeof ev.args === 'object' && Object.keys(ev.args as object).length > 0 ? (
                    <div className="bg-gray-50 rounded p-2 mb-1">
                      <p className="text-xs text-gray-500 mb-1">参数</p>
                      <div className="grid grid-cols-2 gap-1 text-xs">
                        {Object.entries(ev.args as Record<string, unknown>).map(([k, v]) => (
                          <div key={k}><span className="text-gray-500">{k}:</span> <span className="font-mono">{String(v)}</span></div>
                        ))}
                      </div>
                    </div>
                  ) : null}
                  {ev.matched && ev.matched_subscriptions && ev.matched_subscriptions.length > 0 ? (
                    <div className="bg-green-50 border border-green-200 rounded p-2 mt-1">
                      <p className="text-xs text-green-700 font-medium mb-1">✓ 命中 {ev.matched_subscriptions.length} 条订阅</p>
                      {ev.matched_subscriptions.map((sub, si) => (
                        <div key={si} className="flex items-center gap-2 text-xs mt-0.5">
                          <span className="font-medium">{sub.subscription_name}</span>
                          <span className="text-gray-400">→</span>
                          {sub.channels.map(ch => (
                            <span key={ch.id} className={`px-1.5 py-0.5 rounded text-[10px] ${ch.type === 'http' ? 'bg-green-100 text-green-700' : ch.type === 'mq' ? 'bg-yellow-100 text-yellow-700' : 'bg-indigo-100 text-indigo-700'}`}>{ch.name}</span>
                          ))}
                        </div>
                      ))}
                    </div>
                  ) : null}
                  <div className="text-xs text-gray-400 mt-1">
                    tx: {String(ev.tx_hash).slice(0, 16)}... | block: {String(ev.block_number)}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
