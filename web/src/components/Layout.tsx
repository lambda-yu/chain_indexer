import { Link, Outlet, useLocation } from 'react-router-dom'
import { Activity, Box, Link2, Radio, Settings, Upload } from 'lucide-react'
import { cn } from '@/lib/utils'

const NAV = [
  { to: '/', label: 'Dashboard', icon: Activity },
  { to: '/chains', label: 'Chains', icon: Box },
  { to: '/channels', label: 'Channels', icon: Radio },
  { to: '/subscriptions', label: 'Subscriptions', icon: Link2 },
  { to: '/abis', label: 'ABIs', icon: Upload },
  { to: '/events', label: 'Event Stream', icon: Settings },
]

export default function Layout() {
  const { pathname } = useLocation()
  return (
    <div className="flex h-screen">
      <aside className="w-56 border-r bg-gray-50 p-4 flex flex-col gap-1">
        <h1 className="text-lg font-bold mb-4">Chain Indexer</h1>
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
