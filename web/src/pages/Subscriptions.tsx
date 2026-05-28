import { useEffect, useMemo, useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { api } from '@/api/client'
import { Plus, Trash2, Pencil, FlaskConical } from 'lucide-react'

interface Sub { id: string; name: string; chain_id: string; match_kind: string; match_name: string | null; address: string | null; abi_id: string | null; enabled: boolean; arg_filters: Record<string, unknown>; start_block: number | null; last_processed_block: number | null }
interface AbiItem { id: string; name: string; kind: string; body: unknown }

export default function Subscriptions() {
  const qc = useQueryClient()
  const [editing, setEditing] = useState<Sub | null>(null)
  const [showForm, setShowForm] = useState(false)
  const [testingSub, setTestingSub] = useState<Sub | null>(null)
  const { data: subs = [], isLoading } = useQuery<Sub[]>({ queryKey: ['subscriptions'], queryFn: () => api.get('/subscriptions') })
  const { data: abis = [] } = useQuery<AbiItem[]>({ queryKey: ['abis'], queryFn: () => api.get('/abis') })
  const { data: allChannels = [] } = useQuery<{ id: string; name: string; type: string }[]>({ queryKey: ['channels'], queryFn: () => api.get('/channels') })
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
            <th className="py-2 px-2">ABI</th><th className="py-2 px-2">匹配</th><th className="py-2 px-2">渠道</th><th className="py-2 px-2">进度</th><th className="py-2 px-2">启用</th><th className="py-2 px-2">操作</th>
          </tr></thead>
          <tbody>{subs.map(s => (
            <tr key={s.id} className="border-b hover:bg-gray-50">
              <td className="py-2 px-2 font-medium">
                <Link
                  to={`/failed?subscription_id=${s.id}`}
                  className="text-blue-600 hover:text-blue-800 hover:underline"
                  title="查看该订阅的推送记录"
                >
                  {s.name}
                </Link>
              </td>
              <td className="py-2 px-2 font-mono text-xs">{s.chain_id}</td>
              <td className="py-2 px-2"><span className="px-2 py-0.5 rounded text-xs bg-gray-100">{s.match_kind}</span></td>
              <td className="py-2 px-2 text-xs">{s.abi_id ? (abiNameMap[s.abi_id] ?? s.abi_id.slice(0, 8)) : '—'}</td>
              <td className="py-2 px-2">{s.match_name ?? '—'}</td>
              <td className="py-2 px-2"><SubChannelBadges subId={s.id} allChannels={allChannels} /></td>
              <td className="py-2 px-2 text-xs font-mono">
                {s.last_processed_block ? <span className="text-green-600">{s.last_processed_block.toLocaleString()}</span> : <span className="text-gray-400">—</span>}
                {s.start_block ? <span className="text-gray-400 ml-1">/ 起始 {s.start_block.toLocaleString()}</span> : null}
              </td>
              <td className="py-2 px-2">{s.enabled ? <span className="text-green-600">是</span> : <span className="text-gray-400">否</span>}</td>
              <td className="py-2 px-2 flex gap-2">
                <button onClick={() => setTestingSub(s)} className="text-orange-500 hover:text-orange-700" title="测试"><FlaskConical size={14} /></button>
                <button onClick={() => { setEditing(s); setShowForm(true) }} className="text-blue-500 hover:text-blue-700" title="编辑"><Pencil size={14} /></button>
                <button onClick={() => delMut.mutate(s.id)} className="text-red-500 hover:text-red-700" title="删除"><Trash2 size={14} /></button>
              </td>
            </tr>
          ))}</tbody>
        </table>
      )}
      {showForm && <SubForm initial={editing} abis={abis} onClose={() => { setShowForm(false); setEditing(null) }} />}
      {testingSub && <TestSubModal sub={testingSub} onClose={() => setTestingSub(null)} />}
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

interface ChannelItem { id: string; name: string; type: string }

function SubForm({ initial, abis, onClose }: { initial: Sub | null; abis: AbiItem[]; onClose: () => void }) {
  const qc = useQueryClient()
  const isEdit = initial !== null
  const { data: chains = [] } = useQuery<{ id: string }[]>({ queryKey: ['chains'], queryFn: () => api.get('/chains') })
  const { data: allChannels = [] } = useQuery<ChannelItem[]>({ queryKey: ['channels'], queryFn: () => api.get('/channels') })
  const { data: subDetail } = useQuery<{ channel_ids: string[] }>({
    queryKey: ['sub-detail', initial?.id],
    queryFn: () => api.get(`/subscriptions/${initial?.id}`),
    enabled: isEdit,
  })
  const createMut = useMutation({ mutationFn: (d: Record<string, unknown>) => api.post('/subscriptions', d), onSuccess: () => { qc.invalidateQueries({ queryKey: ['subscriptions'] }); onClose() } })
  const updateMut = useMutation({ mutationFn: (d: Record<string, unknown>) => api.put(`/subscriptions/${initial?.id}`, d), onSuccess: () => { qc.invalidateQueries({ queryKey: ['subscriptions'] }); onClose() } })
  const mut = isEdit ? updateMut : createMut

  const [matchKind, setMatchKind] = useState(initial?.match_kind ?? 'native_transfer')
  const [abiId, setAbiId] = useState(initial?.abi_id ?? '')
  const [matchName, setMatchName] = useState(initial?.match_name ?? '')
  const [selectedNames, setSelectedNames] = useState<string[]>(initial?.match_name ? [initial.match_name] : [])
  const [selectedChannels, setSelectedChannels] = useState<string[]>(subDetail?.channel_ids ?? [])
  useEffect(() => { if (subDetail?.channel_ids) setSelectedChannels(subDetail.channel_ids) }, [subDetail])

  const needsAbi = matchKind === 'event' || matchKind === 'call'

  const selectedAbi = useMemo(() => abis.find(a => a.id === abiId), [abis, abiId])
  const nameOptions = useMemo(() => {
    if (!selectedAbi) return []
    if (matchKind === 'event') return extractAbiNames(selectedAbi.body, 'event')
    if (matchKind === 'call') return extractAbiNames(selectedAbi.body, 'function')
    return []
  }, [selectedAbi, matchKind])

  const toggleName = (n: string) => {
    setSelectedNames(prev => prev.includes(n) ? prev.filter(x => x !== n) : [...prev, n])
  }

  const submit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault(); const fd = new FormData(e.currentTarget)
    let af = {}; try { af = JSON.parse(fd.get('arg_filters') as string || '{}') } catch { /* ignore */ }
    const base = {
      chain_id: fd.get('chain_id'),
      address: fd.get('address') || null,
      abi_id: abiId || null,
      match_kind: matchKind,
      arg_filters: af,
      start_block: fd.get('start_block') ? Number(fd.get('start_block')) : null,
      enabled: fd.get('enabled') === 'on',
    }

    if (isEdit) {
      mut.mutate({ ...base, name: fd.get('name'), match_name: selectedNames[0] || matchName || null })
      // Sync channel bindings
      if (subDetail) {
        const current = new Set(subDetail.channel_ids)
        for (const cid of selectedChannels) {
          if (!current.has(cid)) await api.post(`/subscriptions/${initial?.id}/channels`, { channel_id: cid })
        }
        // Note: unbind API not implemented yet, skip removals
      }
      return
    }

    // 新建：多选时为每个事件/函数创建一条订阅
    const names = needsAbi && abiId && selectedNames.length > 0 ? selectedNames : [matchName || null]
    const baseName = fd.get('name') as string
    for (let i = 0; i < names.length; i++) {
      const n = names[i]
      const subName = names.length > 1 ? `${baseName}-${n}` : baseName
      const created = await api.post<{ id: string }>('/subscriptions', { ...base, name: subName, match_name: n })
      // 自动绑定选中的渠道
      for (const cid of selectedChannels) {
        await api.post(`/subscriptions/${created.id}/channels`, { channel_id: cid })
      }
    }
    qc.invalidateQueries({ queryKey: ['subscriptions'] })
    onClose()
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
            <select value={matchKind} onChange={e => { setMatchKind(e.target.value); setMatchName(''); setAbiId(''); setSelectedNames([]) }} className="w-full border rounded px-3 py-1.5 text-sm">
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
              <select value={abiId} onChange={e => { setAbiId(e.target.value); setMatchName(''); setSelectedNames([]) }} className="w-full border rounded px-3 py-1.5 text-sm">
                <option value="">不绑定 ABI（手动输入）</option>
                {abis.map(a => {
                  const ec = extractAbiNames(a.body, 'event').length
                  const fc = extractAbiNames(a.body, 'function').length
                  return <option key={a.id} value={a.id}>{a.name} — {ec} 事件, {fc} 函数</option>
                })}
              </select>
            </div>
            {abiId && nameOptions.length > 0 && (
              <div>
                <label className="text-xs text-gray-500">选择{matchKind === 'event' ? '事件' : '函数'}（可多选）</label>
                <div className="border rounded bg-white p-2 max-h-40 overflow-auto space-y-1 mt-1">
                  {nameOptions.map(n => (
                    <label key={n} className="flex items-center gap-2 text-sm cursor-pointer hover:bg-gray-50 px-1 rounded">
                      <input type="checkbox" checked={selectedNames.includes(n)} onChange={() => toggleName(n)} className="accent-black" />
                      <span className="font-mono text-xs">{n}</span>
                    </label>
                  ))}
                </div>
                {selectedNames.length > 0 && (
                  <p className="text-xs text-blue-600 mt-1">已选 {selectedNames.length} 个{matchKind === 'event' ? '事件' : '函数'}，将创建 {selectedNames.length} 条订阅</p>
                )}
              </div>
            )}
            {abiId && nameOptions.length === 0 && (
              <p className="text-xs text-yellow-600">
                该 ABI 中没有{matchKind === 'event' ? '事件（event）' : '函数（function）'}定义。
                {(() => {
                  if (!selectedAbi) return ''
                  const otherType = matchKind === 'event' ? 'function' : 'event'
                  const otherCount = extractAbiNames(selectedAbi.body, otherType as 'event' | 'function').length
                  return otherCount > 0 ? ` 但有 ${otherCount} 个${otherType === 'event' ? '事件' : '函数'}，请切换类型。` : ''
                })()}
              </p>
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
        <div>
          <label className="text-xs text-gray-500">起始区块（可选，留空从最新开始）</label>
          <input name="start_block" type="number" defaultValue={initial?.start_block ?? ''} placeholder="如 87000000" className="w-full border rounded px-3 py-1.5 text-sm font-mono" />
        </div>
        <div>
          <div className="flex items-center justify-between mb-1">
            <label className="text-xs text-gray-500">参数过滤 JSON</label>
            <ArgFilterExamples onSelect={(v) => {
              const el = document.querySelector<HTMLTextAreaElement>('textarea[name=arg_filters]')
              if (el) el.value = JSON.stringify(v, null, 2)
            }} />
          </div>
          <textarea name="arg_filters" defaultValue={JSON.stringify(initial?.arg_filters ?? {}, null, 2)} placeholder='{}' className="w-full border rounded px-3 py-1.5 text-sm h-20 font-mono" />
        </div>
        <label className="flex items-center gap-2 text-sm"><input name="enabled" type="checkbox" defaultChecked={initial?.enabled ?? true} /> 启用</label>

        {allChannels.length > 0 && (
          <div>
            <label className="text-xs text-gray-500 block mb-1">绑定通知渠道（可多选）</label>
            <div className="border rounded p-2 max-h-28 overflow-auto space-y-1">
              {allChannels.map(ch => (
                <label key={ch.id} className="flex items-center gap-2 text-sm cursor-pointer hover:bg-gray-50 px-1 rounded">
                  <input type="checkbox" checked={selectedChannels.includes(ch.id)}
                    onChange={() => setSelectedChannels(prev => prev.includes(ch.id) ? prev.filter(x => x !== ch.id) : [...prev, ch.id])}
                    className="accent-black" />
                  <span>{ch.name}</span>
                  <span className="text-[10px] text-gray-400 ml-auto">{ch.type}</span>
                </label>
              ))}
            </div>
          </div>
        )}

        <div className="flex gap-2 pt-2">
          <button type="button" onClick={onClose} className="flex-1 border rounded py-1.5 text-sm">取消</button>
          <button type="submit" className="flex-1 bg-black text-white rounded py-1.5 text-sm">{isEdit ? '保存' : '创建'}</button>
        </div>
        {mut.isError && <p className="text-red-500 text-xs">{String(mut.error)}</p>}
      </form>
    </div>
  )
}

function SubChannelBadges({ subId, allChannels }: { subId: string; allChannels: { id: string; name: string; type: string }[] }) {
  const { data } = useQuery<{ channel_ids: string[] }>({
    queryKey: ['sub-channels', subId],
    queryFn: () => api.get(`/subscriptions/${subId}`),
    staleTime: 30000,
  })
  if (!data?.channel_ids?.length) return <span className="text-yellow-600 text-xs">⚠ 未绑定渠道</span>
  const chMap = Object.fromEntries(allChannels.map(c => [c.id, c]))
  return (
    <div className="flex flex-wrap gap-0.5">
      {data.channel_ids.map(cid => {
        const ch = chMap[cid]
        return ch ? <span key={cid} className="px-1 py-0.5 rounded text-[10px] bg-gray-100">{ch.name}</span> : null
      })}
    </div>
  )
}

function TestSubModal({ sub, onClose }: { sub: Sub; onClose: () => void }) {
  const [blockNum, setBlockNum] = useState('')
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState<Record<string, unknown> | null>(null)
  const [error, setError] = useState('')

  const run = async () => {
    if (!blockNum) return
    setLoading(true); setError(''); setResult(null)
    try {
      const res = await api.post<Record<string, unknown>>('/test/test-subscription', {
        subscription_id: sub.id, block_number: Number(blockNum),
      })
      setResult(res)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally { setLoading(false) }
  }

  const matched = (result?.matched as number) ?? 0
  const delivered = (result?.delivered as number) ?? 0
  const events = (result?.events as Record<string, unknown>[]) ?? []
  const channels = (result?.channels as { id: string; name: string; type: string }[]) ?? []

  return (
    <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg p-6 w-[520px] max-h-[85vh] overflow-auto">
        <h3 className="text-lg font-bold mb-1">测试订阅规则</h3>
        <p className="text-xs text-gray-500 mb-3">
          <span className="font-medium">{sub.name}</span> — {sub.match_kind}{sub.match_name ? ` / ${sub.match_name}` : ''} @ {sub.chain_id}
        </p>

        <div className="flex items-end gap-2 mb-4">
          <div className="flex-1">
            <label className="text-xs text-gray-500 block mb-1">区块号</label>
            <input value={blockNum} onChange={e => setBlockNum(e.target.value)} type="number" placeholder="输入区块号" className="w-full border rounded px-3 py-1.5 text-sm" />
          </div>
          <button onClick={run} disabled={loading || !blockNum} className="bg-black text-white px-4 py-1.5 rounded text-sm disabled:opacity-40">
            {loading ? '测试中...' : '解析并推送'}
          </button>
        </div>

        {error && <div className="bg-red-50 border border-red-200 rounded p-2 text-red-700 text-xs mb-3">{error}</div>}

        {result && (
          <div className="space-y-3">
            <div className="flex gap-3">
              <div className="border rounded p-2 text-center flex-1">
                <p className="text-xl font-bold">{result.total_events as number}</p><p className="text-[10px] text-gray-500">解析事件</p>
              </div>
              <div className={`border rounded p-2 text-center flex-1 ${matched > 0 ? 'border-green-300 bg-green-50' : ''}`}>
                <p className="text-xl font-bold text-green-600">{matched}</p><p className="text-[10px] text-gray-500">命中</p>
              </div>
              <div className={`border rounded p-2 text-center flex-1 ${delivered > 0 ? 'border-blue-300 bg-blue-50' : ''}`}>
                <p className="text-xl font-bold text-blue-600">{delivered}</p><p className="text-[10px] text-gray-500">已推送</p>
              </div>
            </div>

            {channels.length > 0 && (
              <div className="text-xs">
                <span className="text-gray-500">推送渠道：</span>
                {channels.map(ch => (
                  <span key={ch.id} className="ml-1 px-1.5 py-0.5 rounded bg-gray-100">{ch.name} ({ch.type})</span>
                ))}
              </div>
            )}
            {channels.length === 0 && <p className="text-xs text-yellow-600">未绑定渠道，仅匹配不推送</p>}

            {matched === 0 && <p className="text-xs text-gray-400">该区块中没有命中此订阅的事件。</p>}

            {events.length > 0 && (
              <div className="space-y-1">
                <p className="text-xs text-gray-500 font-medium">命中事件（最多 50 条）</p>
                {events.map((ev, i) => (
                  <div key={i} className="bg-gray-50 rounded p-2 text-xs">
                    <div className="flex gap-2 items-center">
                      <span className="px-1.5 py-0.5 rounded bg-gray-200 text-[10px]">{String(ev.kind)}</span>
                      {ev.name ? <span className="font-medium">{String(ev.name)}</span> : null}
                      {ev.delivery_error ? <span className="text-red-500 text-[10px]">推送失败</span> : null}
                    </div>
                    {ev.args && typeof ev.args === 'object' && Object.keys(ev.args as object).length > 0 ? (
                      <div className="mt-1 grid grid-cols-2 gap-1">
                        {Object.entries(ev.args as Record<string, unknown>).map(([k, v]) => (
                          <div key={k}><span className="text-gray-400">{k}:</span> <span className="font-mono break-all">{String(v)}</span></div>
                        ))}
                      </div>
                    ) : null}
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        <div className="flex justify-end mt-4">
          <button onClick={onClose} className="border rounded px-4 py-1.5 text-sm">关闭</button>
        </div>
      </div>
    </div>
  )
}

const ARG_FILTER_EXAMPLES: { label: string; desc: string; value: Record<string, unknown> }[] = [
  { label: '精确匹配地址', desc: '只匹配 from 为指定地址', value: { from: '0xABC...123' } },
  { label: '多地址 IN', desc: '匹配 to 在列表中的任一地址', value: { to_in: ['0xAAA...', '0xBBB...'] } },
  { label: '金额范围', desc: '匹配 value >= 1000 且 <= 10000', value: { value_gte: 1000, value_lte: 10000 } },
  { label: '最低金额', desc: '只通知大额转账 (1 ETH)', value: { value_gte: 1000000000000000000 } },
  { label: '组合过滤', desc: '指定发送方 + 最低金额', value: { from: '0xABC...123', value_gte: 100000 } },
  { label: '空（全部匹配）', desc: '不过滤任何参数', value: {} },
]

function ArgFilterExamples({ onSelect }: { onSelect: (v: Record<string, unknown>) => void }) {
  const [open, setOpen] = useState(false)
  return (
    <div className="relative">
      <button type="button" onClick={() => setOpen(!open)} className="text-xs text-blue-500 hover:text-blue-700">
        插入示例 ▾
      </button>
      {open && (
        <div className="absolute right-0 top-5 z-10 bg-white border rounded-lg shadow-lg w-72 py-1">
          {ARG_FILTER_EXAMPLES.map((ex, i) => (
            <button key={i} type="button"
              onClick={() => { onSelect(ex.value); setOpen(false) }}
              className="w-full text-left px-3 py-1.5 hover:bg-gray-50 text-xs">
              <span className="font-medium">{ex.label}</span>
              <span className="text-gray-400 ml-1">— {ex.desc}</span>
              <pre className="text-[10px] text-gray-500 font-mono mt-0.5">{JSON.stringify(ex.value)}</pre>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
