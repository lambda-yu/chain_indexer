import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/api/client'
import { Plus, Trash2 } from 'lucide-react'

interface Sub { id: string; name: string; chain_id: string; match_kind: string; match_name: string | null; address: string | null; enabled: boolean; arg_filters: Record<string, unknown> }

export default function Subscriptions() {
  const qc = useQueryClient()
  const [showForm, setShowForm] = useState(false)
  const { data: subs = [], isLoading } = useQuery<Sub[]>({ queryKey: ['subscriptions'], queryFn: () => api.get('/subscriptions') })
  const delMut = useMutation({ mutationFn: (id: string) => api.del(`/subscriptions/${id}`), onSuccess: () => qc.invalidateQueries({ queryKey: ['subscriptions'] }) })

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-2xl font-bold">Subscriptions</h2>
        <button onClick={() => setShowForm(true)} className="flex items-center gap-1 bg-black text-white px-3 py-1.5 rounded text-sm"><Plus size={14} /> Add</button>
      </div>
      {isLoading ? <p className="text-gray-500">Loading...</p> : (
        <table className="w-full text-sm border-collapse">
          <thead><tr className="border-b text-left text-gray-500"><th className="py-2 px-2">Name</th><th className="py-2 px-2">Chain</th><th className="py-2 px-2">Kind</th><th className="py-2 px-2">Match Name</th><th className="py-2 px-2">Enabled</th><th className="py-2 px-2"></th></tr></thead>
          <tbody>{subs.map(s => (
            <tr key={s.id} className="border-b hover:bg-gray-50">
              <td className="py-2 px-2 font-medium">{s.name}</td>
              <td className="py-2 px-2 font-mono text-xs">{s.chain_id}</td>
              <td className="py-2 px-2"><span className="px-2 py-0.5 rounded text-xs bg-gray-100">{s.match_kind}</span></td>
              <td className="py-2 px-2">{s.match_name ?? '—'}</td>
              <td className="py-2 px-2">{s.enabled ? <span className="text-green-600">Yes</span> : <span className="text-gray-400">No</span>}</td>
              <td className="py-2 px-2"><button onClick={() => delMut.mutate(s.id)} className="text-red-500 hover:text-red-700"><Trash2 size={14} /></button></td>
            </tr>
          ))}</tbody>
        </table>
      )}
      {showForm && <SubForm onClose={() => setShowForm(false)} />}
    </div>
  )
}

function SubForm({ onClose }: { onClose: () => void }) {
  const qc = useQueryClient()
  const { data: chains = [] } = useQuery<{ id: string }[]>({ queryKey: ['chains'], queryFn: () => api.get('/chains') })
  const mut = useMutation({ mutationFn: (d: Record<string, unknown>) => api.post('/subscriptions', d), onSuccess: () => { qc.invalidateQueries({ queryKey: ['subscriptions'] }); onClose() } })
  const submit = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault(); const fd = new FormData(e.currentTarget)
    let af = {}; try { af = JSON.parse(fd.get('arg_filters') as string || '{}') } catch { /* ignore */ }
    mut.mutate({ name: fd.get('name'), chain_id: fd.get('chain_id'), address: fd.get('address') || null, abi_id: null, match_kind: fd.get('match_kind'), match_name: fd.get('match_name') || null, arg_filters: af, enabled: true })
  }
  return (
    <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
      <form onSubmit={submit} className="bg-white rounded-lg p-6 w-96 space-y-3">
        <h3 className="text-lg font-bold">Add Subscription</h3>
        <input name="name" placeholder="Name" required className="w-full border rounded px-3 py-1.5 text-sm" />
        <select name="chain_id" required className="w-full border rounded px-3 py-1.5 text-sm">{chains.map(c => <option key={c.id} value={c.id}>{c.id}</option>)}</select>
        <select name="match_kind" className="w-full border rounded px-3 py-1.5 text-sm">
          <option value="native_transfer">native_transfer</option><option value="token_transfer">token_transfer</option>
          <option value="event">event</option><option value="call">call</option>
        </select>
        <input name="match_name" placeholder="Match name (optional)" className="w-full border rounded px-3 py-1.5 text-sm" />
        <input name="address" placeholder="Contract address (optional)" className="w-full border rounded px-3 py-1.5 text-sm" />
        <textarea name="arg_filters" placeholder='arg_filters JSON (e.g. {"to": "0x..."})' defaultValue="{}" className="w-full border rounded px-3 py-1.5 text-sm h-16 font-mono" />
        <div className="flex gap-2 pt-2"><button type="button" onClick={onClose} className="flex-1 border rounded py-1.5 text-sm">Cancel</button><button type="submit" className="flex-1 bg-black text-white rounded py-1.5 text-sm">Create</button></div>
        {mut.isError && <p className="text-red-500 text-xs">{String(mut.error)}</p>}
      </form>
    </div>
  )
}
