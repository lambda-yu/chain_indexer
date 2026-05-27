import { useMemo, useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/api/client'
import { Plus, Trash2, Pencil } from 'lucide-react'

interface Sub { id: string; name: string; chain_id: string; match_kind: string; match_name: string | null; address: string | null; abi_id: string | null; enabled: boolean; arg_filters: Record<string, unknown> }
interface AbiItem { id: string; name: string; kind: string; body: unknown }

export default function Subscriptions() {
  const qc = useQueryClient()
  const [editing, setEditing] = useState<Sub | null>(null)
  const [showForm, setShowForm] = useState(false)
  const { data: subs = [], isLoading } = useQuery<Sub[]>({ queryKey: ['subscriptions'], queryFn: () => api.get('/subscriptions') })
  const { data: abis = [] } = useQuery<AbiItem[]>({ queryKey: ['abis'], queryFn: () => api.get('/abis') })
  const delMut = useMutation({ mutationFn: (id: string) => api.del(`/subscriptions/${id}`), onSuccess: () => qc.invalidateQueries({ queryKey: ['subscriptions'] }) })

  const abiNameMap = useMemo(() => Object.fromEntries(abis.map(a => [a.id, a.name])), [abis])

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-2xl font-bold">订阅规则</h2>
        <button onClick={() => { setEditing(null); setShowForm(true) }} className="flex items-center gap-1 bg-black text-white px-3 py-1.5 rounded text-sm"><Plus size={14} /> 添加</button>
      </div>
      {isLoading ? <p className="text-gray-500">加载中...</p> : (
        <table className="w-full text-sm border-collapse">
          <thead><tr className="border-b text-left text-gray-500">
            <th className="py-2 px-2">名称</th><th className="py-2 px-2">链</th><th className="py-2 px-2">类型</th>
            <th className="py-2 px-2">ABI</th><th className="py-2 px-2">匹配</th><th className="py-2 px-2">启用</th><th className="py-2 px-2">操作</th>
          </tr></thead>
          <tbody>{subs.map(s => (
            <tr key={s.id} className="border-b hover:bg-gray-50">
              <td className="py-2 px-2 font-medium">{s.name}</td>
              <td className="py-2 px-2 font-mono text-xs">{s.chain_id}</td>
              <td className="py-2 px-2"><span className="px-2 py-0.5 rounded text-xs bg-gray-100">{s.match_kind}</span></td>
              <td className="py-2 px-2 text-xs">{s.abi_id ? (abiNameMap[s.abi_id] ?? s.abi_id.slice(0, 8)) : '—'}</td>
              <td className="py-2 px-2">{s.match_name ?? '—'}</td>
              <td className="py-2 px-2">{s.enabled ? <span className="text-green-600">是</span> : <span className="text-gray-400">否</span>}</td>
              <td className="py-2 px-2 flex gap-2">
                <button onClick={() => { setEditing(s); setShowForm(true) }} className="text-blue-500 hover:text-blue-700"><Pencil size={14} /></button>
                <button onClick={() => delMut.mutate(s.id)} className="text-red-500 hover:text-red-700"><Trash2 size={14} /></button>
              </td>
            </tr>
          ))}</tbody>
        </table>
      )}
      {showForm && <SubForm initial={editing} abis={abis} onClose={() => { setShowForm(false); setEditing(null) }} />}
    </div>
  )
}

function extractAbiNames(body: unknown, type: 'event' | 'function'): string[] {
  if (Array.isArray(body)) {
    return body.filter((e: Record<string, string>) => e.type === type).map((e: Record<string, string>) => e.name)
  }
  // Solana IDL
  if (body && typeof body === 'object') {
    const obj = body as Record<string, unknown>
    if (type === 'event' && Array.isArray(obj.events)) return (obj.events as { name: string }[]).map(e => e.name)
    if (type === 'function' && Array.isArray(obj.instructions)) return (obj.instructions as { name: string }[]).map(e => e.name)
  }
  return []
}

function SubForm({ initial, abis, onClose }: { initial: Sub | null; abis: AbiItem[]; onClose: () => void }) {
  const qc = useQueryClient()
  const isEdit = initial !== null
  const { data: chains = [] } = useQuery<{ id: string }[]>({ queryKey: ['chains'], queryFn: () => api.get('/chains') })
  const createMut = useMutation({ mutationFn: (d: Record<string, unknown>) => api.post('/subscriptions', d), onSuccess: () => { qc.invalidateQueries({ queryKey: ['subscriptions'] }); onClose() } })
  const updateMut = useMutation({ mutationFn: (d: Record<string, unknown>) => api.put(`/subscriptions/${initial?.id}`, d), onSuccess: () => { qc.invalidateQueries({ queryKey: ['subscriptions'] }); onClose() } })
  const mut = isEdit ? updateMut : createMut

  const [matchKind, setMatchKind] = useState(initial?.match_kind ?? 'native_transfer')
  const [abiId, setAbiId] = useState(initial?.abi_id ?? '')
  const [matchName, setMatchName] = useState(initial?.match_name ?? '')

  const needsAbi = matchKind === 'event' || matchKind === 'call'

  const selectedAbi = useMemo(() => abis.find(a => a.id === abiId), [abis, abiId])
  const nameOptions = useMemo(() => {
    if (!selectedAbi) return []
    if (matchKind === 'event') return extractAbiNames(selectedAbi.body, 'event')
    if (matchKind === 'call') return extractAbiNames(selectedAbi.body, 'function')
    return []
  }, [selectedAbi, matchKind])

  const submit = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault(); const fd = new FormData(e.currentTarget)
    let af = {}; try { af = JSON.parse(fd.get('arg_filters') as string || '{}') } catch { /* ignore */ }
    mut.mutate({
      name: fd.get('name'),
      chain_id: fd.get('chain_id'),
      address: fd.get('address') || null,
      abi_id: abiId || null,
      match_kind: matchKind,
      match_name: matchName || null,
      arg_filters: af,
      enabled: fd.get('enabled') === 'on',
    })
  }

  return (
    <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
      <form onSubmit={submit} className="bg-white rounded-lg p-6 w-[460px] space-y-3 max-h-[90vh] overflow-auto">
        <h3 className="text-lg font-bold">{isEdit ? '编辑订阅' : '添加订阅'}</h3>

        <input name="name" defaultValue={initial?.name ?? ''} placeholder="订阅名称" required className="w-full border rounded px-3 py-1.5 text-sm" />

        <div className="grid grid-cols-2 gap-2">
          <div>
            <label className="text-xs text-gray-500">链</label>
            <select name="chain_id" defaultValue={initial?.chain_id ?? ''} required className="w-full border rounded px-3 py-1.5 text-sm">
              {chains.map(c => <option key={c.id} value={c.id}>{c.id}</option>)}
            </select>
          </div>
          <div>
            <label className="text-xs text-gray-500">事件类型</label>
            <select value={matchKind} onChange={e => { setMatchKind(e.target.value); setMatchName(''); setAbiId('') }} className="w-full border rounded px-3 py-1.5 text-sm">
              <option value="native_transfer">native_transfer</option>
              <option value="token_transfer">token_transfer</option>
              <option value="event">event（合约事件）</option>
              <option value="call">call（合约调用）</option>
            </select>
          </div>
        </div>

        {needsAbi && (
          <div className="border rounded p-3 bg-blue-50 space-y-2">
            <p className="text-xs text-blue-600 font-medium">从 ABI 选择{matchKind === 'event' ? '事件' : '函数'}</p>
            <div>
              <label className="text-xs text-gray-500">选择 ABI</label>
              <select value={abiId} onChange={e => { setAbiId(e.target.value); setMatchName('') }} className="w-full border rounded px-3 py-1.5 text-sm">
                <option value="">不绑定 ABI（手动输入）</option>
                {abis.map(a => <option key={a.id} value={a.id}>{a.name} ({a.kind})</option>)}
              </select>
            </div>
            {abiId && nameOptions.length > 0 && (
              <div>
                <label className="text-xs text-gray-500">选择{matchKind === 'event' ? '事件' : '函数'}</label>
                <select value={matchName} onChange={e => setMatchName(e.target.value)} className="w-full border rounded px-3 py-1.5 text-sm">
                  <option value="">全部匹配（不限名称）</option>
                  {nameOptions.map(n => <option key={n} value={n}>{n}</option>)}
                </select>
              </div>
            )}
            {abiId && nameOptions.length === 0 && (
              <p className="text-xs text-gray-400">该 ABI 中没有{matchKind === 'event' ? '事件' : '函数'}定义</p>
            )}
            {!abiId && (
              <input value={matchName} onChange={e => setMatchName(e.target.value)} placeholder="手动输入匹配名称（可选）" className="w-full border rounded px-3 py-1.5 text-sm" />
            )}
          </div>
        )}

        {!needsAbi && (
          <input value={matchName} onChange={e => setMatchName(e.target.value)} placeholder="匹配名称（可选）" className="w-full border rounded px-3 py-1.5 text-sm" />
        )}

        <input name="address" defaultValue={initial?.address ?? ''} placeholder="合约地址（可选）" className="w-full border rounded px-3 py-1.5 text-sm font-mono" />
        <textarea name="arg_filters" defaultValue={JSON.stringify(initial?.arg_filters ?? {}, null, 2)} placeholder='参数过滤 JSON' className="w-full border rounded px-3 py-1.5 text-sm h-16 font-mono" />
        <label className="flex items-center gap-2 text-sm"><input name="enabled" type="checkbox" defaultChecked={initial?.enabled ?? true} /> 启用</label>

        <div className="flex gap-2 pt-2">
          <button type="button" onClick={onClose} className="flex-1 border rounded py-1.5 text-sm">取消</button>
          <button type="submit" className="flex-1 bg-black text-white rounded py-1.5 text-sm">{isEdit ? '保存' : '创建'}</button>
        </div>
        {mut.isError && <p className="text-red-500 text-xs">{String(mut.error)}</p>}
      </form>
    </div>
  )
}
