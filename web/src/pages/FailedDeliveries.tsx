import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Link, useSearchParams } from 'react-router-dom'
import { api } from '@/api/client'
import { RefreshCw, Check, Trash2, ChevronDown, ChevronRight, AlertTriangle, X } from 'lucide-react'

interface FailedDelivery {
  id: string; subscription_id: string; channel_id: string; chain_id: string
  event_payload: Record<string, unknown>; error: string; attempts: number
  status: string; created_at: string; resolved_at: string | null
}

interface SubItem { id: string; name: string }

export default function FailedDeliveries() {
  const qc = useQueryClient()
  const [params, setParams] = useSearchParams()
  const subFilter = params.get('subscription_id')

  const { data: items = [], isLoading } = useQuery<FailedDelivery[]>({
    queryKey: ['failed-deliveries', subFilter],
    queryFn: () => api.get(subFilter ? `/failed-deliveries?subscription_id=${subFilter}` : '/failed-deliveries'),
    refetchInterval: 10000,
  })
  const { data: subs = [] } = useQuery<SubItem[]>({
    queryKey: ['subscriptions'], queryFn: () => api.get('/subscriptions'),
    enabled: !!subFilter, staleTime: 30000,
  })
  const subName = subFilter ? (subs.find(s => s.id === subFilter)?.name ?? subFilter.slice(0, 8) + '...') : null

  const retryMut = useMutation({ mutationFn: (id: string) => api.post(`/failed-deliveries/${id}/retry`, {}), onSuccess: () => qc.invalidateQueries({ queryKey: ['failed-deliveries'] }) })
  const resolveMut = useMutation({ mutationFn: (id: string) => api.post(`/failed-deliveries/${id}/resolve`, {}), onSuccess: () => qc.invalidateQueries({ queryKey: ['failed-deliveries'] }) })
  const delMut = useMutation({ mutationFn: (id: string) => api.del(`/failed-deliveries/${id}`), onSuccess: () => qc.invalidateQueries({ queryKey: ['failed-deliveries'] }) })
  const [expanded, setExpanded] = useState<Set<string>>(new Set())
  const toggle = (id: string) => setExpanded(prev => { const n = new Set(prev); n.has(id) ? n.delete(id) : n.add(id); return n })

  const failed = items.filter(i => i.status === 'failed')

  const clearFilter = () => { setParams({}); qc.invalidateQueries({ queryKey: ['failed-deliveries'] }) }

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-2xl font-bold">{subFilter ? '推送记录' : '失败投递（死信队列）'}</h2>
        {failed.length > 0 && <span className="px-2 py-1 rounded bg-red-100 text-red-700 text-sm font-medium">{failed.length} 条待处理</span>}
      </div>

      {subFilter && (
        <div className="mb-3 flex items-center gap-2 text-sm bg-blue-50 border border-blue-200 rounded px-3 py-1.5">
          <span className="text-gray-500">订阅:</span>
          <span className="font-medium">{subName}</span>
          <Link to="/subscriptions" className="text-blue-600 hover:underline text-xs ml-2">← 返回订阅列表</Link>
          <button onClick={clearFilter} className="ml-auto text-gray-500 hover:text-gray-700" title="清除过滤显示全部">
            <X size={14} />
          </button>
        </div>
      )}

      {isLoading ? <p className="text-gray-500">加载中...</p> : items.length === 0 ? (
        <div className="text-center py-12 text-gray-400">
          <AlertTriangle size={32} className="mx-auto mb-2 opacity-50" />
          <p>暂无失败投递记录</p>
        </div>
      ) : (
        <div className="space-y-2">
          {items.map(item => (
            <div key={item.id} className={`border rounded-lg ${item.status === 'failed' ? 'border-red-200 bg-red-50/30' : 'border-green-200 bg-green-50/30'}`}>
              <div className="flex items-center gap-2 px-3 py-2 cursor-pointer" onClick={() => toggle(item.id)}>
                {expanded.has(item.id) ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${item.status === 'failed' ? 'bg-red-100 text-red-700' : 'bg-green-100 text-green-700'}`}>{item.status === 'failed' ? '失败' : '已解决'}</span>
                <span className="text-sm font-mono truncate max-w-40">{item.subscription_id.slice(0, 8)}...</span>
                <span className="text-xs text-gray-400">→</span>
                <span className="text-sm font-mono truncate max-w-40">{item.channel_id.slice(0, 8)}...</span>
                <span className="text-xs text-gray-400 ml-auto">{item.attempts} 次</span>
                <span className="text-xs text-gray-400">{new Date(item.created_at).toLocaleString()}</span>
              </div>
              {expanded.has(item.id) && (
                <div className="px-3 pb-3 space-y-2">
                  <div className="bg-white rounded p-2 text-xs">
                    <p className="text-gray-500 mb-1">错误信息</p>
                    <p className="text-red-600 font-mono break-all">{item.error}</p>
                  </div>
                  <div className="bg-white rounded p-2 text-xs">
                    <p className="text-gray-500 mb-1">事件负载</p>
                    <pre className="overflow-auto max-h-40 text-[11px] font-mono">{JSON.stringify(item.event_payload, null, 2)}</pre>
                  </div>
                  <div className="flex gap-2">
                    {item.status === 'failed' && <>
                      <button onClick={() => retryMut.mutate(item.id)} className="flex items-center gap-1 bg-black text-white px-3 py-1 rounded text-xs"><RefreshCw size={12} /> 重推</button>
                      <button onClick={() => resolveMut.mutate(item.id)} className="flex items-center gap-1 border px-3 py-1 rounded text-xs"><Check size={12} /> 标记已解决</button>
                    </>}
                    <button onClick={() => delMut.mutate(item.id)} className="flex items-center gap-1 text-red-500 border border-red-200 px-3 py-1 rounded text-xs"><Trash2 size={12} /> 删除</button>
                  </div>
                  {retryMut.isError && <p className="text-red-500 text-xs">{String(retryMut.error)}</p>}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
