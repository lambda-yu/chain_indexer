import { BrowserRouter, Route, Routes } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import Layout from '@/components/Layout'
import Dashboard from '@/pages/Dashboard'
import Chains from '@/pages/Chains'
import Channels from '@/pages/Channels'
import Subscriptions from '@/pages/Subscriptions'
import Abis from '@/pages/Abis'
import BlockTest from '@/pages/BlockTest'
import EventStream from '@/pages/EventStream'
import DeliveryRecords from '@/pages/DeliveryRecords'
import Logs from '@/pages/Logs'

const queryClient = new QueryClient()

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Routes>
          <Route element={<Layout />}>
            <Route path="/" element={<Dashboard />} />
            <Route path="/chains" element={<Chains />} />
            <Route path="/channels" element={<Channels />} />
            <Route path="/subscriptions" element={<Subscriptions />} />
            <Route path="/abis" element={<Abis />} />
            <Route path="/test" element={<BlockTest />} />
            <Route path="/deliveries" element={<DeliveryRecords />} />
            <Route path="/failed" element={<DeliveryRecords />} />
            <Route path="/logs" element={<Logs />} />
            <Route path="/events" element={<EventStream />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  )
}
