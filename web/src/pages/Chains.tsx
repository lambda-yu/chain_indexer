import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/api/client'
import { Plus, Trash2, Pencil, Wifi, WifiOff } from 'lucide-react'

interface Chain {
  id: string; kind: string; rpc_http: string; rpc_ws: string | null
  confirmations: number; poll_interval_ms: number; commitment: string | null
  trace_internal_calls: boolean | null; enabled: boolean
}

export default function Chains() {
  const qc = useQueryClient()
  const [editing, setEditing] = useState<Chain | null>(null)
  const [showForm, setShowForm] = useState(false)
  const { data: chains = [], isLoading } = useQuery<Chain[]>({ queryKey: ['chains'], queryFn: () => api.get('/chains') })
  const deleteMut = useMutation({ mutationFn: (id: string) => api.del(`/chains/${id}`), onSuccess: () => qc.invalidateQueries({ queryKey: ['chains'] }) })

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-2xl font-bold">链配置</h2>
        <button onClick={() => { setEditing(null); setShowForm(true) }} className="flex items-center gap-1 bg-black text-white px-3 py-1.5 rounded text-sm"><Plus size={14} /> 添加链</button>
      </div>
      {isLoading ? <p className="text-gray-500">加载中...</p> : (
        <table className="w-full text-sm border-collapse">
          <thead><tr className="border-b text-left text-gray-500">
            <th className="py-2 px-2">ID</th><th className="py-2 px-2">类型</th><th className="py-2 px-2">RPC</th>
            <th className="py-2 px-2">确认数</th><th className="py-2 px-2">轮询</th><th className="py-2 px-2">状态</th><th className="py-2 px-2">操作</th>
          </tr></thead>
          <tbody>{chains.map((c) => (
            <tr key={c.id} className="border-b hover:bg-gray-50">
              <td className="py-2 px-2 font-mono text-xs">{c.id}</td>
              <td className="py-2 px-2"><span className={`px-2 py-0.5 rounded text-xs font-medium ${c.kind === 'evm' ? 'bg-blue-100 text-blue-700' : 'bg-purple-100 text-purple-700'}`}>{c.kind}</span></td>
              <td className="py-2 px-2 font-mono text-xs truncate max-w-48">{c.rpc_http}</td>
              <td className="py-2 px-2">{c.kind === 'solana' ? c.commitment : c.confirmations}</td>
              <td className="py-2 px-2">{c.poll_interval_ms}ms</td>
              <td className="py-2 px-2">{c.enabled ? <span className="flex items-center gap-1 text-green-600"><Wifi size={14} />运行</span> : <span className="flex items-center gap-1 text-gray-400"><WifiOff size={14} />停用</span>}</td>
              <td className="py-2 px-2 flex gap-2">
                <button onClick={() => { setEditing(c); setShowForm(true) }} className="text-blue-500 hover:text-blue-700"><Pencil size={14} /></button>
                <button onClick={() => deleteMut.mutate(c.id)} className="text-red-500 hover:text-red-700"><Trash2 size={14} /></button>
              </td>
            </tr>
          ))}</tbody>
        </table>
      )}
      {showForm && <ChainForm initial={editing} onClose={() => { setShowForm(false); setEditing(null) }} />}
    </div>
  )
}

function ChainForm({ initial, onClose }: { initial: Chain | null; onClose: () => void }) {
  const qc = useQueryClient()
  const isEdit = initial !== null
  const [kind, setKind] = useState<'evm' | 'solana'>((initial?.kind as 'evm' | 'solana') || 'evm')
  const createMut = useMutation({ mutationFn: (d: Record<string, unknown>) => api.post('/chains', d), onSuccess: () => { qc.invalidateQueries({ queryKey: ['chains'] }); onClose() } })
  const updateMut = useMutation({ mutationFn: (d: Record<string, unknown>) => api.put(`/chains/${initial?.id}`, d), onSuccess: () => { qc.invalidateQueries({ queryKey: ['chains'] }); onClose() } })
  const mut = isEdit ? updateMut : createMut
  const submit = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault(); const fd = new FormData(e.currentTarget)
    mut.mutate({ id: fd.get('id') || initial?.id, kind, rpc_http: fd.get('rpc_http'), rpc_ws: fd.get('rpc_ws') || null, confirmations: kind === 'evm' ? Number(fd.get('confirmations')) : 0, commitment: kind === 'solana' ? fd.get('commitment') : null, poll_interval_ms: Number(fd.get('poll_interval_ms')), enabled: fd.get('enabled') === 'on' })
  }
  return (
    <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
      <form onSubmit={submit} className="bg-white rounded-lg p-6 w-96 space-y-3">
        <h3 className="text-lg font-bold">{isEdit ? '编辑链' : '添加链'}</h3>
        <input name="id" defaultValue={initial?.id ?? ''} placeholder="链 ID（如 eth-mainnet）" required disabled={isEdit} className="w-full border rounded px-3 py-1.5 text-sm disabled:bg-gray-100" />
        <div className="flex gap-2">{(['evm','solana'] as const).map(k => <button key={k} type="button" onClick={() => !isEdit && setKind(k)} className={`flex-1 py-1.5 rounded text-sm ${kind===k?'bg-black text-white':'border'} ${isEdit?'opacity-60':''}`}>{k.toUpperCase()}</button>)}</div>
        <input name="rpc_http" defaultValue={initial?.rpc_http ?? ''} placeholder="RPC HTTP 地址" required className="w-full border rounded px-3 py-1.5 text-sm" />
        <input name="rpc_ws" defaultValue={initial?.rpc_ws ?? ''} placeholder="RPC WS 地址（可选）" className="w-full border rounded px-3 py-1.5 text-sm" />
        {kind === 'evm' ? <input name="confirmations" type="number" defaultValue={initial?.confirmations ?? 12} className="w-full border rounded px-3 py-1.5 text-sm" /> : <select name="commitment" defaultValue={initial?.commitment ?? 'confirmed'} className="w-full border rounded px-3 py-1.5 text-sm"><option value="confirmed">confirmed</option><option value="finalized">finalized</option></select>}
        <input name="poll_interval_ms" type="number" defaultValue={initial?.poll_interval_ms ?? 3000} placeholder="轮询间隔 (ms)" className="w-full border rounded px-3 py-1.5 text-sm" />
        <label className="flex items-center gap-2 text-sm"><input name="enabled" type="checkbox" defaultChecked={initial?.enabled ?? true} /> 启用</label>
        <div className="flex gap-2 pt-2"><button type="button" onClick={onClose} className="flex-1 border rounded py-1.5 text-sm">取消</button><button type="submit" className="flex-1 bg-black text-white rounded py-1.5 text-sm">{isEdit ? '保存' : '创建'}</button></div>
        {mut.isError && <p className="text-red-500 text-xs">{String(mut.error)}</p>}
      </form>
    </div>
  )
}
