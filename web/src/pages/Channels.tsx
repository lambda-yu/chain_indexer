import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/api/client'
import { Plus, Trash2, Pencil } from 'lucide-react'

interface Channel { id: string; name: string; type: string; config: Record<string, unknown> }

export default function Channels() {
  const qc = useQueryClient()
  const [editing, setEditing] = useState<Channel | null>(null)
  const [showForm, setShowForm] = useState(false)
  const { data: channels = [], isLoading } = useQuery<Channel[]>({ queryKey: ['channels'], queryFn: () => api.get('/channels') })
  const delMut = useMutation({ mutationFn: (id: string) => api.del(`/channels/${id}`), onSuccess: () => qc.invalidateQueries({ queryKey: ['channels'] }) })
  const typeBadge = (t: string) => ({ http: 'bg-green-100 text-green-700', mq: 'bg-yellow-100 text-yellow-700', ws: 'bg-indigo-100 text-indigo-700' }[t] ?? 'bg-gray-100')

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-2xl font-bold">通知渠道</h2>
        <button onClick={() => { setEditing(null); setShowForm(true) }} className="flex items-center gap-1 bg-black text-white px-3 py-1.5 rounded text-sm"><Plus size={14} /> 添加渠道</button>
      </div>
      {isLoading ? <p className="text-gray-500">加载中...</p> : (
        <table className="w-full text-sm border-collapse">
          <thead><tr className="border-b text-left text-gray-500"><th className="py-2 px-2">名称</th><th className="py-2 px-2">类型</th><th className="py-2 px-2">配置</th><th className="py-2 px-2">操作</th></tr></thead>
          <tbody>{channels.map(c => (
            <tr key={c.id} className="border-b hover:bg-gray-50">
              <td className="py-2 px-2 font-medium">{c.name}</td>
              <td className="py-2 px-2"><span className={`px-2 py-0.5 rounded text-xs font-medium ${typeBadge(c.type)}`}>{c.type}</span></td>
              <td className="py-2 px-2 font-mono text-xs truncate max-w-64">{JSON.stringify(c.config)}</td>
              <td className="py-2 px-2 flex gap-2">
                <button onClick={() => { setEditing(c); setShowForm(true) }} className="text-blue-500 hover:text-blue-700"><Pencil size={14} /></button>
                <button onClick={() => delMut.mutate(c.id)} className="text-red-500 hover:text-red-700"><Trash2 size={14} /></button>
              </td>
            </tr>
          ))}</tbody>
        </table>
      )}
      {showForm && <ChannelForm initial={editing} onClose={() => { setShowForm(false); setEditing(null) }} />}
    </div>
  )
}

function ChannelForm({ initial, onClose }: { initial: Channel | null; onClose: () => void }) {
  const qc = useQueryClient()
  const isEdit = initial !== null
  const [type, setType] = useState<'http'|'mq'|'ws'>((initial?.type as 'http'|'mq'|'ws') || 'http')
  const createMut = useMutation({ mutationFn: (d: Record<string, unknown>) => api.post('/channels', d), onSuccess: () => { qc.invalidateQueries({ queryKey: ['channels'] }); onClose() } })
  const updateMut = useMutation({ mutationFn: (d: Record<string, unknown>) => api.put(`/channels/${initial?.id}`, d), onSuccess: () => { qc.invalidateQueries({ queryKey: ['channels'] }); onClose() } })
  const mut = isEdit ? updateMut : createMut
  const cfg = initial?.config ?? {}
  const submit = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault(); const fd = new FormData(e.currentTarget)
    let config: Record<string, unknown> = {}
    if (type === 'http') config = { url: fd.get('url'), method: fd.get('method') || 'POST' }
    else if (type === 'mq') config = { stream: fd.get('stream'), ...(fd.get('maxlen') ? { maxlen: Number(fd.get('maxlen')) } : {}) }
    else config = { ws_fanout_channel: fd.get('ws_fanout_channel') }
    mut.mutate({ name: fd.get('name'), type, config })
  }
  return (
    <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
      <form onSubmit={submit} className="bg-white rounded-lg p-6 w-96 space-y-3">
        <h3 className="text-lg font-bold">{isEdit ? '编辑渠道' : '添加渠道'}</h3>
        <input name="name" defaultValue={initial?.name ?? ''} placeholder="渠道名称" required className="w-full border rounded px-3 py-1.5 text-sm" />
        <div className="flex gap-2">{(['http','mq','ws'] as const).map(t => <button key={t} type="button" onClick={() => !isEdit && setType(t)} className={`flex-1 py-1.5 rounded text-sm ${type===t?'bg-black text-white':'border'} ${isEdit?'opacity-60':''}`}>{t.toUpperCase()}</button>)}</div>
        {type === 'http' && <><input name="url" defaultValue={String(cfg.url ?? '')} placeholder="Webhook 地址" required className="w-full border rounded px-3 py-1.5 text-sm" /><input name="method" defaultValue={String(cfg.method ?? 'POST')} placeholder="请求方法" className="w-full border rounded px-3 py-1.5 text-sm" /></>}
        {type === 'mq' && <><input name="stream" defaultValue={String(cfg.stream ?? '')} placeholder="Redis Stream 名称" required className="w-full border rounded px-3 py-1.5 text-sm" /><input name="maxlen" type="number" defaultValue={cfg.maxlen ? Number(cfg.maxlen) : undefined} placeholder="最大长度（可选）" className="w-full border rounded px-3 py-1.5 text-sm" /></>}
        {type === 'ws' && <input name="ws_fanout_channel" defaultValue={String(cfg.ws_fanout_channel ?? '')} placeholder="广播频道名称" required className="w-full border rounded px-3 py-1.5 text-sm" />}
        <div className="flex gap-2 pt-2"><button type="button" onClick={onClose} className="flex-1 border rounded py-1.5 text-sm">取消</button><button type="submit" className="flex-1 bg-black text-white rounded py-1.5 text-sm">{isEdit ? '保存' : '创建'}</button></div>
        {mut.isError && <p className="text-red-500 text-xs">{String(mut.error)}</p>}
      </form>
    </div>
  )
}
