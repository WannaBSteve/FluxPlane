import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import type { Job } from '@/lib/api'

export default function ExecutionChart({ jobs }: { jobs: Job[] }) {
  const data = [
    { name: '已完成', value: jobs.filter(job => job.status === 'succeeded').length },
    { name: '运行中', value: jobs.filter(job => job.status === 'running').length },
    { name: '等待中', value: jobs.filter(job => job.status === 'pending').length },
    { name: '失败', value: jobs.filter(job => job.status === 'failed').length },
  ]

  return <div className="chart-wrap"><ResponsiveContainer width="100%" height="100%">
    <BarChart data={data} margin={{ top: 10, right: 6, bottom: 2, left: -26 }}>
      <CartesianGrid vertical={false} stroke="#e9edf5" />
      <XAxis dataKey="name" axisLine={false} tickLine={false} tick={{ fontSize: 11, fill: '#8b9ab0' }} />
      <YAxis allowDecimals={false} axisLine={false} tickLine={false} tick={{ fontSize: 11, fill: '#8b9ab0' }} />
      <Tooltip />
      <Bar dataKey="value" fill="#456ee9" radius={[4, 4, 0, 0]} maxBarSize={35} />
    </BarChart>
  </ResponsiveContainer></div>
}
