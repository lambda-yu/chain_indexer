import { Link, Outlet, useLocation } from 'react-router-dom'
import { Activity, Box, Link2, Radio, Settings, Upload, FlaskConical, AlertTriangle } from 'lucide-react'
import { cn } from '@/lib/utils'

const NAV = [
  { to: '/', label: '仪表盘', icon: Activity },
  { to: '/chains', label: '链配置', icon: Box },
  { to: '/channels', label: '通知渠道', icon: Radio },
  { to: '/subscriptions', label: '订阅规则', icon: Link2 },
  { to: '/abis', label: 'ABI 管理', icon: Upload },
  { to: '/test', label: '区块测试', icon: FlaskConical },
  { to: '/failed', label: '失败投递', icon: AlertTriangle },
  { to: '/events', label: '实时事件', icon: Settings },
]

export default function Layout() {
  const { pathname } = useLocation()
  return (
    <div className="flex h-screen">
      <aside className="w-56 border-r bg-gray-50 p-4 flex flex-col gap-1">
        <h1 className="text-lg font-bold mb-4">链索引器</h1>
        {NAV.map(({ to, label, icon: Icon }) => (
          <Link
            key={to}
            to={to}
            className={cn(
              'flex items-center gap-2 rounded px-3 py-2 text-sm',
              pathname === to ? 'bg-gray-200 font-medium' : 'hover:bg-gray-100',
            )}
          >
            <Icon size={16} />
            {label}
          </Link>
        ))}
      </aside>
      <main className="flex-1 overflow-auto p-6">
        <Outlet />
      </main>
    </div>
  )
}
