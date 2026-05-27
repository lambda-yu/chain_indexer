import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/api/client'
import { Plus, Trash2, Eye, Download } from 'lucide-react'

interface Abi { id: string; name: string; kind: string; body: unknown }

export default function Abis() {
  const qc = useQueryClient()
  const [showForm, setShowForm] = useState(false)
  const [showFetch, setShowFetch] = useState(false)
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
        <h2 className="text-2xl font-bold">ABI 管理</h2>
        <div className="flex gap-2">
          <button onClick={() => setShowFetch(true)} className="flex items-center gap-1 border px-3 py-1.5 rounded text-sm"><Download size={14} /> 链上拉取 IDL</button>
          <button onClick={() => setShowForm(true)} className="flex items-center gap-1 bg-black text-white px-3 py-1.5 rounded text-sm"><Plus size={14} /> 上传 ABI</button>
        </div>
      </div>
      {isLoading ? <p className="text-gray-500">加载中...</p> : (
        <table className="w-full text-sm border-collapse">
          <thead><tr className="border-b text-left text-gray-500"><th className="py-2 px-2">名称</th><th className="py-2 px-2">类型</th><th className="py-2 px-2">ID</th><th className="py-2 px-2"></th></tr></thead>
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
      {showFetch && <FetchIdlForm onClose={() => setShowFetch(false)} />}
      {preview && (
        <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50" onClick={() => setPreview(null)}>
          <div className="bg-white rounded-lg p-6 w-[500px] max-h-[80vh] overflow-auto" onClick={e => e.stopPropagation()}>
            <h3 className="text-lg font-bold mb-3">{preview.name} — 预览</h3>
            {(() => { const { events, functions } = extractNames(preview.body); return (<>
              {events.length > 0 && <div className="mb-3"><p className="text-xs text-gray-500 mb-1">事件 ({events.length})</p>{events.map(n => <span key={n} className="inline-block px-2 py-0.5 rounded bg-orange-100 text-orange-700 text-xs mr-1 mb-1">{n}</span>)}</div>}
              {functions.length > 0 && <div className="mb-3"><p className="text-xs text-gray-500 mb-1">函数 ({functions.length})</p>{functions.map(n => <span key={n} className="inline-block px-2 py-0.5 rounded bg-teal-100 text-teal-700 text-xs mr-1 mb-1">{n}</span>)}</div>}
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
        <h3 className="text-lg font-bold">上传 ABI</h3>
        <input name="name" placeholder="ABI 名称" required className="w-full border rounded px-3 py-1.5 text-sm" />
        <select name="kind" className="w-full border rounded px-3 py-1.5 text-sm"><option value="evm_abi">EVM ABI</option><option value="solana_idl">Solana IDL</option></select>
        <div><input type="file" accept=".json" onChange={handleFile} className="text-sm" /><span className="text-xs text-gray-400 ml-2">或在下方粘贴</span></div>
        <textarea value={body} onChange={e => setBody(e.target.value)} placeholder="粘贴 ABI/IDL JSON" className="w-full border rounded px-3 py-1.5 text-sm h-32 font-mono" />
        <div className="flex gap-2 pt-2"><button type="button" onClick={onClose} className="flex-1 border rounded py-1.5 text-sm">取消</button><button type="submit" className="flex-1 bg-black text-white rounded py-1.5 text-sm">上传</button></div>
        {mut.isError && <p className="text-red-500 text-xs">{String(mut.error)}</p>}
      </form>
    </div>
  )
}

function FetchIdlForm({ onClose }: { onClose: () => void }) {
  const qc = useQueryClient()
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const submit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault(); setError(''); setLoading(true)
    const fd = new FormData(e.currentTarget)
    const programId = fd.get('program_id') as string
    const rpcUrl = fd.get('rpc_url') as string
    const name = fd.get('name') as string || undefined
    try {
      const params = new URLSearchParams({ program_id: programId, rpc_url: rpcUrl })
      if (name) params.set('name', name)
      await api.post(`/abis/fetch-idl?${params.toString()}`, {})
      qc.invalidateQueries({ queryKey: ['abis'] })
      onClose()
    } catch (err) {
      setError(String(err))
    } finally { setLoading(false) }
  }
  return (
    <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
      <form onSubmit={submit} className="bg-white rounded-lg p-6 w-[480px] space-y-3">
        <h3 className="text-lg font-bold">从链上拉取 Solana IDL</h3>
        <p className="text-xs text-gray-500">输入 Anchor 程序的 Program ID，系统将自动从链上 IDL 账户拉取并解压 IDL。</p>
        <input name="program_id" placeholder="Program ID（如 TokenkegQfeZy...）" required className="w-full border rounded px-3 py-1.5 text-sm font-mono" />
        <input name="rpc_url" placeholder="Solana RPC URL" defaultValue="https://api.mainnet-beta.solana.com" required className="w-full border rounded px-3 py-1.5 text-sm" />
        <input name="name" placeholder="名称（可选，默认自动生成）" className="w-full border rounded px-3 py-1.5 text-sm" />
        <div className="flex gap-2 pt-2">
          <button type="button" onClick={onClose} className="flex-1 border rounded py-1.5 text-sm">取消</button>
          <button type="submit" disabled={loading} className="flex-1 bg-black text-white rounded py-1.5 text-sm disabled:opacity-50">
            {loading ? '拉取中...' : '拉取 IDL'}
          </button>
        </div>
        {error && <p className="text-red-500 text-xs">{error}</p>}
      </form>
    </div>
  )
}
