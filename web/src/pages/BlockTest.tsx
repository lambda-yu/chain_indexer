import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '@/api/client'
import { Play, Loader2 } from 'lucide-react'

interface Chain { id: string; kind: string }
interface ParseResult {
  chain_id: string; kind: string; block_number: number
  tx_count?: number; log_count?: number; events: Record<string, unknown>[]; error?: string
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
      setError(String(err))
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
              <p className="text-2xl font-bold">{result.events.length}</p>
              <p className="text-xs text-gray-500">解析事件</p>
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
                <div key={i} className="border rounded-lg p-3">
                  <div className="flex items-center gap-2 mb-2">
                    <span className="px-2 py-0.5 rounded text-xs font-medium bg-gray-100">{String(ev.kind)}</span>
                    {ev.name ? <span className="text-sm font-medium">{String(ev.name)}</span> : null}
                    {ev.contract ? <span className="text-xs font-mono text-gray-400 truncate max-w-64">{String(ev.contract)}</span> : null}
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
