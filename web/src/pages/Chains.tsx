import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/api/client'
import { Plus, Trash2, Wifi, WifiOff } from 'lucide-react'

interface Chain {
  id: string; kind: string; rpc_http: string; rpc_ws: string | null
  confirmations: number; poll_interval_ms: number; commitment: string | null
  trace_internal_calls: boolean | null; enabled: boolean
}

export default function Chains() {
  const qc = useQueryClient()
  const [showForm, setShowForm] = useState(false)
  const { data: chains = [], isLoading } = useQuery<Chain[]>({ queryKey: ['chains'], queryFn: () => api.get('/chains') })
  const deleteMut = useMutation({ mutationFn: (id: string) => api.del(`/chains/${id}`), onSuccess: () => qc.invalidateQueries({ queryKey: ['chains'] }) })

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-2xl font-bold">Chains</h2>
        <button onClick={() => setShowForm(true)} className="flex items-center gap-1 bg-black text-white px-3 py-1.5 rounded text-sm"><Plus size={14} /> Add Chain</button>
      </div>
      {isLoading ? <p className="text-gray-500">Loading...</p> : (
        <table className="w-full text-sm border-collapse">
          <thead><tr className="border-b text-left text-gray-500">
            <th className="py-2 px-2">ID</th><th className="py-2 px-2">Kind</th><th className="py-2 px-2">RPC</th>
            <th className="py-2 px-2">Conf.</th><th className="py-2 px-2">Poll</th><th className="py-2 px-2">Status</th><th className="py-2 px-2"></th>
          </tr></thead>
          <tbody>{chains.map((c) => (
            <tr key={c.id} className="border-b hover:bg-gray-50">
              <td className="py-2 px-2 font-mono text-xs">{c.id}</td>
              <td className="py-2 px-2"><span className={`px-2 py-0.5 rounded text-xs font-medium ${c.kind === 'evm' ? 'bg-blue-100 text-blue-700' : 'bg-purple-100 text-purple-700'}`}>{c.kind}</span></td>
              <td className="py-2 px-2 font-mono text-xs truncate max-w-48">{c.rpc_http}</td>
              <td className="py-2 px-2">{c.kind === 'solana' ? c.commitment : c.confirmations}</td>
              <td className="py-2 px-2">{c.poll_interval_ms}ms</td>
              <td className="py-2 px-2">{c.enabled ? <span className="flex items-center gap-1 text-green-600"><Wifi size={14} />Active</span> : <span className="flex items-center gap-1 text-gray-400"><WifiOff size={14} />Off</span>}</td>
              <td className="py-2 px-2"><button onClick={() => deleteMut.mutate(c.id)} className="text-red-500 hover:text-red-700"><Trash2 size={14} /></button></td>
            </tr>
          ))}</tbody>
        </table>
      )}
      {showForm && <ChainForm onClose={() => setShowForm(false)} />}
    </div>
  )
}

function ChainForm({ onClose }: { onClose: () => void }) {
  const qc = useQueryClient()
  const [kind, setKind] = useState<'evm' | 'solana'>('evm')
  const mut = useMutation({ mutationFn: (d: Record<string, unknown>) => api.post('/chains', d), onSuccess: () => { qc.invalidateQueries({ queryKey: ['chains'] }); onClose() } })
  const submit = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault(); const fd = new FormData(e.currentTarget)
    mut.mutate({ id: fd.get('id'), kind, rpc_http: fd.get('rpc_http'), rpc_ws: fd.get('rpc_ws') || null, confirmations: kind === 'evm' ? Number(fd.get('confirmations')) : 0, commitment: kind === 'solana' ? fd.get('commitment') : null, poll_interval_ms: Number(fd.get('poll_interval_ms')), enabled: true })
  }
  return (
    <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
      <form onSubmit={submit} className="bg-white rounded-lg p-6 w-96 space-y-3">
        <h3 className="text-lg font-bold">Add Chain</h3>
        <input name="id" placeholder="Chain ID" required className="w-full border rounded px-3 py-1.5 text-sm" />
        <div className="flex gap-2">{(['evm','solana'] as const).map(k => <button key={k} type="button" onClick={() => setKind(k)} className={`flex-1 py-1.5 rounded text-sm ${kind===k?'bg-black text-white':'border'}`}>{k.toUpperCase()}</button>)}</div>
        <input name="rpc_http" placeholder="RPC HTTP URL" required className="w-full border rounded px-3 py-1.5 text-sm" />
        <input name="rpc_ws" placeholder="RPC WS URL (optional)" className="w-full border rounded px-3 py-1.5 text-sm" />
        {kind === 'evm' ? <input name="confirmations" type="number" defaultValue={12} className="w-full border rounded px-3 py-1.5 text-sm" /> : <select name="commitment" className="w-full border rounded px-3 py-1.5 text-sm"><option value="confirmed">confirmed</option><option value="finalized">finalized</option></select>}
        <input name="poll_interval_ms" type="number" defaultValue={3000} className="w-full border rounded px-3 py-1.5 text-sm" />
        <div className="flex gap-2 pt-2"><button type="button" onClick={onClose} className="flex-1 border rounded py-1.5 text-sm">Cancel</button><button type="submit" className="flex-1 bg-black text-white rounded py-1.5 text-sm">Create</button></div>
        {mut.isError && <p className="text-red-500 text-xs">{String(mut.error)}</p>}
      </form>
    </div>
  )
}
