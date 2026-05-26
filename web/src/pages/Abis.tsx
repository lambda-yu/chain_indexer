import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/api/client'
import { Plus, Trash2, Eye } from 'lucide-react'

interface Abi { id: string; name: string; kind: string; body: unknown }

export default function Abis() {
  const qc = useQueryClient()
  const [showForm, setShowForm] = useState(false)
  const [preview, setPreview] = useState<Abi | null>(null)
  const { data: abis = [], isLoading } = useQuery<Abi[]>({ queryKey: ['abis'], queryFn: () => api.get('/abis') })
  const delMut = useMutation({ mutationFn: (id: string) => api.del(`/abis/${id}`), onSuccess: () => qc.invalidateQueries({ queryKey: ['abis'] }) })

  const extractNames = (body: unknown): { events: string[]; functions: string[] } => {
    const entries = Array.isArray(body) ? body : []
    return {
      events: entries.filter((e: Record<string, string>) => e.type === 'event').map((e: Record<string, string>) => e.name),
      functions: entries.filter((e: Record<string, string>) => e.type === 'function').map((e: Record<string, string>) => e.name),
    }
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-2xl font-bold">ABIs</h2>
        <button onClick={() => setShowForm(true)} className="flex items-center gap-1 bg-black text-white px-3 py-1.5 rounded text-sm"><Plus size={14} /> Upload ABI</button>
      </div>
      {isLoading ? <p className="text-gray-500">Loading...</p> : (
        <table className="w-full text-sm border-collapse">
          <thead><tr className="border-b text-left text-gray-500"><th className="py-2 px-2">Name</th><th className="py-2 px-2">Kind</th><th className="py-2 px-2">ID</th><th className="py-2 px-2"></th></tr></thead>
          <tbody>{abis.map(a => (
            <tr key={a.id} className="border-b hover:bg-gray-50">
              <td className="py-2 px-2 font-medium">{a.name}</td>
              <td className="py-2 px-2"><span className={`px-2 py-0.5 rounded text-xs font-medium ${a.kind === 'evm_abi' ? 'bg-blue-100 text-blue-700' : 'bg-purple-100 text-purple-700'}`}>{a.kind}</span></td>
              <td className="py-2 px-2 font-mono text-xs">{a.id}</td>
              <td className="py-2 px-2 flex gap-2">
                <button onClick={() => setPreview(a)} className="text-gray-500 hover:text-gray-700"><Eye size={14} /></button>
                <button onClick={() => delMut.mutate(a.id)} className="text-red-500 hover:text-red-700"><Trash2 size={14} /></button>
              </td>
            </tr>
          ))}</tbody>
        </table>
      )}
      {showForm && <AbiForm onClose={() => setShowForm(false)} />}
      {preview && (
        <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50" onClick={() => setPreview(null)}>
          <div className="bg-white rounded-lg p-6 w-[500px] max-h-[80vh] overflow-auto" onClick={e => e.stopPropagation()}>
            <h3 className="text-lg font-bold mb-3">{preview.name} — Preview</h3>
            {(() => { const { events, functions } = extractNames(preview.body); return (<>
              {events.length > 0 && <div className="mb-3"><p className="text-xs text-gray-500 mb-1">Events ({events.length})</p>{events.map(n => <span key={n} className="inline-block px-2 py-0.5 rounded bg-orange-100 text-orange-700 text-xs mr-1 mb-1">{n}</span>)}</div>}
              {functions.length > 0 && <div className="mb-3"><p className="text-xs text-gray-500 mb-1">Functions ({functions.length})</p>{functions.map(n => <span key={n} className="inline-block px-2 py-0.5 rounded bg-teal-100 text-teal-700 text-xs mr-1 mb-1">{n}</span>)}</div>}
            </>)})()}
            <pre className="bg-gray-50 rounded p-3 text-xs overflow-auto max-h-60 mt-3">{JSON.stringify(preview.body, null, 2)}</pre>
          </div>
        </div>
      )}
    </div>
  )
}

function AbiForm({ onClose }: { onClose: () => void }) {
  const qc = useQueryClient()
  const [body, setBody] = useState('')
  const mut = useMutation({ mutationFn: (d: Record<string, unknown>) => api.post('/abis', d), onSuccess: () => { qc.invalidateQueries({ queryKey: ['abis'] }); onClose() } })
  const submit = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault(); const fd = new FormData(e.currentTarget)
    let parsed; try { parsed = JSON.parse(body) } catch { return }
    mut.mutate({ name: fd.get('name'), kind: fd.get('kind'), body: parsed })
  }
  const handleFile = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]; if (!file) return
    const reader = new FileReader(); reader.onload = () => setBody(reader.result as string); reader.readAsText(file)
  }
  return (
    <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
      <form onSubmit={submit} className="bg-white rounded-lg p-6 w-[500px] space-y-3">
        <h3 className="text-lg font-bold">Upload ABI</h3>
        <input name="name" placeholder="ABI name" required className="w-full border rounded px-3 py-1.5 text-sm" />
        <select name="kind" className="w-full border rounded px-3 py-1.5 text-sm"><option value="evm_abi">EVM ABI</option><option value="solana_idl">Solana IDL</option></select>
        <div><input type="file" accept=".json" onChange={handleFile} className="text-sm" /><span className="text-xs text-gray-400 ml-2">or paste below</span></div>
        <textarea value={body} onChange={e => setBody(e.target.value)} placeholder="Paste ABI/IDL JSON here" className="w-full border rounded px-3 py-1.5 text-sm h-32 font-mono" />
        <div className="flex gap-2 pt-2"><button type="button" onClick={onClose} className="flex-1 border rounded py-1.5 text-sm">Cancel</button><button type="submit" className="flex-1 bg-black text-white rounded py-1.5 text-sm">Upload</button></div>
        {mut.isError && <p className="text-red-500 text-xs">{String(mut.error)}</p>}
      </form>
    </div>
  )
}
