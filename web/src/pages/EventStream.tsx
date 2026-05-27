import { useCallback, useEffect, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '@/api/client'
import { Pause, Play, Plug, PlugZap } from 'lucide-react'

interface Channel { id: string; name: string; type: string }
interface EventMsg { timestamp: string; data: Record<string, unknown> }

const MAX_EVENTS = 200

export default function EventStream() {
  const { data: channels = [] } = useQuery<Channel[]>({ queryKey: ['channels'], queryFn: () => api.get('/channels') })
  const wsChannels = channels.filter(c => c.type === 'ws')
  const [selectedId, setSelectedId] = useState('')
  const [connected, setConnected] = useState(false)
  const [paused, setPaused] = useState(false)
  const [events, setEvents] = useState<EventMsg[]>([])
  const wsRef = useRef<WebSocket | null>(null)
  const pausedRef = useRef(false)
  pausedRef.current = paused

  const connect = useCallback(() => {
    if (!selectedId || wsRef.current) return
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:'
    const ws = new WebSocket(`${proto}//${location.host}/ws?channel_id=${selectedId}`)
    ws.onopen = () => setConnected(true)
    ws.onclose = () => { setConnected(false); wsRef.current = null }
    ws.onmessage = (e) => {
      if (pausedRef.current) return
      try {
        const data = JSON.parse(e.data)
        setEvents(prev => [{ timestamp: new Date().toISOString(), data }, ...prev].slice(0, MAX_EVENTS))
      } catch { /* ignore malformed */ }
    }
    wsRef.current = ws
  }, [selectedId])

  const disconnect = useCallback(() => {
    wsRef.current?.close()
    wsRef.current = null
    setConnected(false)
  }, [])

  useEffect(() => () => { wsRef.current?.close() }, [])

  return (
    <div>
      <h2 className="text-2xl font-bold mb-4">实时事件</h2>
      <div className="flex items-center gap-3 mb-4">
        <select value={selectedId} onChange={e => setSelectedId(e.target.value)} className="border rounded px-3 py-1.5 text-sm" disabled={connected}>
          <option value="">选择 WS 渠道...</option>
          {wsChannels.map(c => <option key={c.id} value={c.id}>{c.name} ({c.id.slice(0, 8)})</option>)}
        </select>
        {connected ? (
          <button onClick={disconnect} className="flex items-center gap-1 bg-red-500 text-white px-3 py-1.5 rounded text-sm"><PlugZap size={14} /> 断开</button>
        ) : (
          <button onClick={connect} disabled={!selectedId} className="flex items-center gap-1 bg-black text-white px-3 py-1.5 rounded text-sm disabled:opacity-40"><Plug size={14} /> 连接</button>
        )}
        {connected && (
          <button onClick={() => setPaused(!paused)} className="flex items-center gap-1 border px-3 py-1.5 rounded text-sm">
            {paused ? <><Play size={14} /> 继续</> : <><Pause size={14} /> 暂停</>}
          </button>
        )}
        <span className="text-xs text-gray-400">{events.length} 条事件</span>
      </div>

      {!connected && events.length === 0 && <p className="text-gray-400 text-sm">选择一个 WS 渠道并连接以查看实时事件。</p>}

      <div className="space-y-2">
        {events.map((ev, i) => (
          <div key={i} className="border rounded-lg p-3 text-xs">
            <div className="flex items-center gap-2 mb-1">
              <span className="text-gray-400">{ev.timestamp}</span>
              {(() => { const evt = ev.data.event as Record<string, unknown> | undefined; if (!evt || typeof evt !== 'object') return null; return (<>
                <span className="px-1.5 py-0.5 rounded bg-gray-100 font-medium">{String(evt.kind)}</span>
                {evt.name && <span className="text-gray-600">{String(evt.name)}</span>}
              </>)})()}
            </div>
            <pre className="bg-gray-50 rounded p-2 overflow-auto max-h-32 text-[11px]">{JSON.stringify(ev.data, null, 2)}</pre>
          </div>
        ))}
      </div>
    </div>
  )
}
