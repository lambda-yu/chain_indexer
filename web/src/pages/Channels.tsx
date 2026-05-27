import { useMemo, useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/api/client'
import { Plus, Trash2, Pencil } from 'lucide-react'

interface Channel { id: string; name: string; type: string; config: Record<string, unknown> }

type ChannelCategory = 'http' | 'mq' | 'ws'
type MqDriver = 'mq' | 'kafka' | 'rabbitmq'
type ChannelType = 'http' | 'mq' | 'kafka' | 'rabbitmq' | 'ws'

const MQ_TYPES = new Set<string>(['mq', 'kafka', 'rabbitmq'])

function categoryOf(t: string): ChannelCategory {
  if (MQ_TYPES.has(t)) return 'mq'
  if (t === 'ws') return 'ws'
  return 'http'
}

function driverLabel(t: string): string {
  return { mq: 'Redis Streams', kafka: 'Kafka', rabbitmq: 'RabbitMQ', http: 'HTTP Webhook', ws: 'WebSocket' }[t] ?? t
}

const BADGE = (t: string) => ({ http: 'bg-green-100 text-green-700', mq: 'bg-yellow-100 text-yellow-700', ws: 'bg-indigo-100 text-indigo-700', kafka: 'bg-orange-100 text-orange-700', rabbitmq: 'bg-pink-100 text-pink-700' }[t] ?? 'bg-gray-100')

export default function Channels() {
  const qc = useQueryClient()
  const [editing, setEditing] = useState<Channel | null>(null)
  const [showForm, setShowForm] = useState(false)
  const { data: channels = [], isLoading } = useQuery<Channel[]>({ queryKey: ['channels'], queryFn: () => api.get('/channels') })
  const delMut = useMutation({ mutationFn: (id: string) => api.del(`/channels/${id}`), onSuccess: () => qc.invalidateQueries({ queryKey: ['channels'] }) })

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
              <td className="py-2 px-2"><span className={`px-2 py-0.5 rounded text-xs font-medium ${BADGE(c.type)}`}>{driverLabel(c.type)}</span></td>
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
  const initType = (initial?.type ?? 'http') as ChannelType
  const [category, setCategory] = useState<ChannelCategory>(categoryOf(initType))
  const [mqDriver, setMqDriver] = useState<MqDriver>(MQ_TYPES.has(initType) ? initType as MqDriver : 'mq')
  const actualType: ChannelType = category === 'mq' ? mqDriver : category

  const createMut = useMutation({ mutationFn: (d: Record<string, unknown>) => api.post('/channels', d), onSuccess: () => { qc.invalidateQueries({ queryKey: ['channels'] }); onClose() } })
  const updateMut = useMutation({ mutationFn: (d: Record<string, unknown>) => api.put(`/channels/${initial?.id}`, d), onSuccess: () => { qc.invalidateQueries({ queryKey: ['channels'] }); onClose() } })
  const mut = isEdit ? updateMut : createMut
  const cfg = initial?.config ?? {}

  // Parse redis_url back to host/port/db/password for edit回填
  const redisParts = useMemo(() => {
    const url = String(cfg.redis_url ?? '')
    if (!url) return { host: '', port: '', db: '', password: '' }
    try {
      const m = url.match(/^redis:\/\/(?::(.+)@)?([^:/?]+):?(\d+)?\/(\d+)?/)
      return { host: m?.[2] ?? '', port: m?.[3] ?? '6379', db: m?.[4] ?? '0', password: m?.[1] ?? '' }
    } catch { return { host: '', port: '', db: '', password: '' } }
  }, [cfg.redis_url])

  const submit = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault(); const fd = new FormData(e.currentTarget)
    let config: Record<string, unknown> = {}
    if (actualType === 'http') {
      config = { url: fd.get('url'), method: fd.get('method') || 'POST', ...(fd.get('hmac_secret') ? { hmac_secret: fd.get('hmac_secret') } : {}), ...(fd.get('timeout_seconds') ? { timeout_seconds: Number(fd.get('timeout_seconds')) } : {}) }
    } else if (actualType === 'mq') {
      const host = fd.get('redis_host') as string
      const port = fd.get('redis_port') as string
      const db = fd.get('redis_db') as string
      const pwd = fd.get('redis_password') as string
      let redis_url = ''
      if (host) {
        const auth = pwd ? `:${pwd}@` : ''
        redis_url = `redis://${auth}${host}:${port || '6379'}/${db || '0'}`
      }
      config = { stream: fd.get('stream'), ...(redis_url ? { redis_url } : {}), ...(fd.get('maxlen') ? { maxlen: Number(fd.get('maxlen')) } : {}) }
    } else if (actualType === 'kafka') {
      config = { bootstrap_servers: fd.get('bootstrap_servers'), topic: fd.get('topic'), ...(fd.get('key') ? { key: fd.get('key') } : {}), ...(fd.get('compression_type') && fd.get('compression_type') !== 'none' ? { compression_type: fd.get('compression_type') } : {}) }
    } else if (actualType === 'rabbitmq') {
      config = { url: fd.get('rmq_url'), ...(fd.get('exchange') ? { exchange: fd.get('exchange') } : {}), ...(fd.get('routing_key') ? { routing_key: fd.get('routing_key') } : {}), ...(fd.get('queue') ? { queue: fd.get('queue') } : {}) }
    } else {
      config = { ws_fanout_channel: fd.get('ws_fanout_channel') }
    }
    mut.mutate({ name: fd.get('name'), type: actualType, config })
  }

  return (
    <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
      <form onSubmit={submit} className="bg-white rounded-lg p-6 w-[440px] space-y-3 max-h-[90vh] overflow-auto">
        <h3 className="text-lg font-bold">{isEdit ? '编辑渠道' : '添加渠道'}</h3>
        <input name="name" defaultValue={initial?.name ?? ''} placeholder="渠道名称" required className="w-full border rounded px-3 py-1.5 text-sm" />

        {/* 一级分类 */}
        <div>
          <label className="text-xs text-gray-500 mb-1 block">渠道类型</label>
          <div className="flex gap-2">
            {([['http', 'HTTP Webhook'], ['mq', '消息队列 (MQ)'], ['ws', 'WebSocket']] as const).map(([k, label]) => (
              <button key={k} type="button" onClick={() => !isEdit && setCategory(k)}
                className={`flex-1 py-2 rounded text-sm ${category === k ? 'bg-black text-white' : 'border hover:bg-gray-50'} ${isEdit ? 'opacity-60' : ''}`}>
                {label}
              </button>
            ))}
          </div>
        </div>

        {/* MQ 二级 driver 选择 */}
        {category === 'mq' && (
          <div className="border rounded p-3 bg-blue-50 space-y-2">
            <label className="text-xs text-blue-600 font-medium block">选择 MQ 驱动</label>
            <div className="flex gap-2">
              {([['mq', 'Redis Streams', 'bg-yellow-100 text-yellow-700'], ['kafka', 'Kafka', 'bg-orange-100 text-orange-700'], ['rabbitmq', 'RabbitMQ', 'bg-pink-100 text-pink-700']] as const).map(([k, label, activeColor]) => (
                <button key={k} type="button" onClick={() => !isEdit && setMqDriver(k)}
                  className={`flex-1 py-1.5 rounded text-sm border ${mqDriver === k ? activeColor + ' font-medium border-current' : 'hover:bg-gray-50'} ${isEdit ? 'opacity-60' : ''}`}>
                  {label}
                </button>
              ))}
            </div>

            {/* Redis Streams 配置 */}
            {mqDriver === 'mq' && <>
              <p className="text-xs text-gray-500">Redis 连接（可选，留空用 Worker 默认连接）</p>
              <div className="grid grid-cols-3 gap-2">
                <input name="redis_host" defaultValue={redisParts.host} placeholder="Host" className="border rounded px-3 py-1.5 text-sm" />
                <input name="redis_port" type="number" defaultValue={redisParts.port || undefined} placeholder="端口 (6379)" className="border rounded px-3 py-1.5 text-sm" />
                <input name="redis_db" type="number" defaultValue={redisParts.db || undefined} placeholder="DB (0)" className="border rounded px-3 py-1.5 text-sm" />
              </div>
              <input name="redis_password" defaultValue={redisParts.password} placeholder="Redis 密码（可选）" type="password" className="w-full border rounded px-3 py-1.5 text-sm" />
              <input name="stream" defaultValue={String(cfg.stream ?? '')} placeholder="Stream 名称" required className="w-full border rounded px-3 py-1.5 text-sm" />
              <input name="maxlen" type="number" defaultValue={cfg.maxlen ? Number(cfg.maxlen) : undefined} placeholder="最大长度（可选，MAXLEN ~ N）" className="w-full border rounded px-3 py-1.5 text-sm" />
            </>}

            {/* Kafka 配置 */}
            {mqDriver === 'kafka' && <>
              <input name="bootstrap_servers" defaultValue={String(cfg.bootstrap_servers ?? '')} placeholder="Broker 地址（如 localhost:9092）" required className="w-full border rounded px-3 py-1.5 text-sm" />
              <input name="topic" defaultValue={String(cfg.topic ?? '')} placeholder="Topic" required className="w-full border rounded px-3 py-1.5 text-sm" />
              <input name="key" defaultValue={String(cfg.key ?? '')} placeholder="Partition Key（可选）" className="w-full border rounded px-3 py-1.5 text-sm" />
              <div>
                <label className="text-xs text-gray-500">压缩</label>
                <select name="compression_type" defaultValue={String(cfg.compression_type ?? 'none')} className="w-full border rounded px-3 py-1.5 text-sm">
                  <option value="none">无压缩</option><option value="gzip">gzip</option><option value="snappy">snappy</option><option value="lz4">lz4</option><option value="zstd">zstd</option>
                </select>
              </div>
            </>}

            {/* RabbitMQ 配置 */}
            {mqDriver === 'rabbitmq' && <>
              <input name="rmq_url" defaultValue={String(cfg.url ?? '')} placeholder="AMQP URL（如 amqp://guest:guest@localhost/）" required className="w-full border rounded px-3 py-1.5 text-sm" />
              <input name="exchange" defaultValue={String(cfg.exchange ?? '')} placeholder="Exchange（可选，留空用 default）" className="w-full border rounded px-3 py-1.5 text-sm" />
              <input name="routing_key" defaultValue={String(cfg.routing_key ?? '')} placeholder="Routing Key（可选）" className="w-full border rounded px-3 py-1.5 text-sm" />
              <input name="queue" defaultValue={String(cfg.queue ?? '')} placeholder="Queue（可选）" className="w-full border rounded px-3 py-1.5 text-sm" />
            </>}
          </div>
        )}

        {/* HTTP 配置 */}
        {category === 'http' && <>
          <input name="url" defaultValue={String(cfg.url ?? '')} placeholder="Webhook URL" required className="w-full border rounded px-3 py-1.5 text-sm" />
          <div className="grid grid-cols-2 gap-2">
            <input name="method" defaultValue={String(cfg.method ?? 'POST')} placeholder="方法 (POST)" className="border rounded px-3 py-1.5 text-sm" />
            <input name="timeout_seconds" type="number" defaultValue={cfg.timeout_seconds ? Number(cfg.timeout_seconds) : undefined} placeholder="超时秒数" className="border rounded px-3 py-1.5 text-sm" />
          </div>
          <input name="hmac_secret" defaultValue={String(cfg.hmac_secret ?? '')} placeholder="HMAC Secret（可选，用于签名验证）" className="w-full border rounded px-3 py-1.5 text-sm" />
        </>}

        {/* WebSocket 配置 */}
        {category === 'ws' && <input name="ws_fanout_channel" defaultValue={String(cfg.ws_fanout_channel ?? '')} placeholder="Redis Pub/Sub 广播频道名" required className="w-full border rounded px-3 py-1.5 text-sm" />}

        <div className="flex gap-2 pt-2">
          <button type="button" onClick={onClose} className="flex-1 border rounded py-1.5 text-sm">取消</button>
          <button type="submit" className="flex-1 bg-black text-white rounded py-1.5 text-sm">{isEdit ? '保存' : '创建'}</button>
        </div>
        {mut.isError && <p className="text-red-500 text-xs">{String(mut.error)}</p>}
      </form>
    </div>
  )
}
