import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/api/client'
import { RefreshCw, Trash2, Search } from 'lucide-react'

interface LogEntry { event?: string; level?: string; timestamp?: string; [key: string]: unknown }

const LEVEL_COLOR: Record<string, string> = {
  debug: 'text-gray-400', info: 'text-blue-600', warning: 'text-yellow-600', error: 'text-red-600',
}

export default function Logs() {
  const qc = useQueryClient()
  const { data: logs = [], isLoading } = useQuery<LogEntry[]>({ queryKey: ['logs'], queryFn: () => api.get('/logs?limit=300'), refetchInterval: 3000 })
  const clearMut = useMutation({ mutationFn: () => api.del('/logs'), onSuccess: () => qc.invalidateQueries({ queryKey: ['logs'] }) })
  const [search, setSearch] = useState('')

  const filtered = search ? logs.filter(l => JSON.stringify(l).toLowerCase().includes(search.toLowerCase())) : logs

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-2xl font-bold">Worker 日志</h2>
        <div className="flex gap-2">
          <button onClick={() => qc.invalidateQueries({ queryKey: ['logs'] })} className="flex items-center gap-1 border px-3 py-1.5 rounded text-sm"><RefreshCw size={14} /> 刷新</button>
          <button onClick={() => clearMut.mutate()} className="flex items-center gap-1 border border-red-200 text-red-500 px-3 py-1.5 rounded text-sm"><Trash2 size={14} /> 清空</button>
        </div>
      </div>

      <div className="relative mb-3">
        <Search size={14} className="absolute left-2.5 top-2 text-gray-400" />
        <input value={search} onChange={e => setSearch(e.target.value)} placeholder="搜索日志..." className="w-full border rounded pl-8 pr-3 py-1.5 text-sm" />
      </div>

      <p className="text-xs text-gray-400 mb-2">{filtered.length} 条日志（3 秒自动刷新）</p>

      {isLoading ? <p className="text-gray-500">加载中...</p> : filtered.length === 0 ? (
        <p className="text-gray-400 text-sm">暂无日志。Worker 启动后日志会自动出现。</p>
      ) : (
        <div className="border rounded-lg overflow-hidden">
          <table className="w-full text-xs font-mono">
            <thead><tr className="bg-gray-50 text-left text-gray-500">
              <th className="py-1.5 px-2 w-44">时间</th>
              <th className="py-1.5 px-2 w-16">级别</th>
              <th className="py-1.5 px-2">事件</th>
              <th className="py-1.5 px-2">详情</th>
            </tr></thead>
            <tbody>{filtered.map((log, i) => {
              const { event, level, timestamp, ...rest } = log
              const details = Object.entries(rest).filter(([k]) => k !== 'message')
              return (
                <tr key={i} className="border-t hover:bg-gray-50">
                  <td className="py-1 px-2 text-gray-400">{timestamp ? new Date(String(timestamp)).toLocaleTimeString() : '—'}</td>
                  <td className={`py-1 px-2 font-medium ${LEVEL_COLOR[String(level)] ?? ''}`}>{String(level ?? '').toUpperCase()}</td>
                  <td className="py-1 px-2 font-medium">{String(event ?? log.message ?? '')}</td>
                  <td className="py-1 px-2 text-gray-500 truncate max-w-96">
                    {details.length > 0 ? details.map(([k, v]) => `${k}=${JSON.stringify(v)}`).join(' ') : ''}
                  </td>
                </tr>
              )
            })}</tbody>
          </table>
        </div>
      )}
    </div>
  )
}
