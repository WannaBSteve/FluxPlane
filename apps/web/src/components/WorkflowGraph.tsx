import { useMemo } from 'react'
import { Background, Controls, Position, ReactFlow, type Edge, type Node } from '@xyflow/react'
import type { Workflow } from '@/lib/api'

const labels: Record<string, string> = { sync: '数据同步', refresh: '特征刷新', query: '分析查询' }
const states: Record<string, string> = { pending: '等待中', running: '运行中', succeeded: '已完成',
  failed: '失败', cancelling: '取消中', cancelled: '已取消' }

export default function WorkflowGraph({ workflow, targets }: { workflow: Workflow; targets: Record<string, string> }) {
  const flow = useMemo(() => {
    const depth: Record<string, number> = {}
    workflow.jobs.forEach(job => { depth[job.node_key ?? job.id] = 0 })
    for (let i = 0; i < workflow.jobs.length; i++) {
      let changed = false
      workflow.edges.forEach(edge => {
        const next = depth[edge.from] + 1
        if (next > depth[edge.to]) { depth[edge.to] = next; changed = true }
      })
      if (!changed) break
    }
    const slots: Record<number, number> = {}
    const nodes: Node[] = workflow.jobs.map(job => {
      const key = job.node_key ?? job.id
      const column = depth[key] ?? 0
      const row = slots[column] ?? 0
      slots[column] = row + 1
      return { id: key, position: { x: 24 + column * 235, y: 25 + row * 105 },
        sourcePosition: Position.Right, targetPosition: Position.Left,
        data: { label: <div className="flow-node workflow-node"><strong>{job.target_id ? targets[job.target_id] ?? key : job.kind === 'query' ? '分析结果' : key}</strong><span>{labels[job.kind] ?? job.kind} · {states[job.status] ?? job.status}</span></div> },
        style: { border: 'none', background: 'none', padding: 0 } }
    })
    const edges: Edge[] = workflow.edges.map(edge => ({
      id: `${edge.from}-${edge.to}`, source: edge.from, target: edge.to,
      animated: workflow.jobs.some(job => job.node_key === edge.to && job.status === 'running'),
      style: { stroke: '#88a4d9', strokeWidth: 1.7 },
    }))
    return { nodes, edges }
  }, [workflow, targets])

  return <div className="graph-wrap workflow-graph"><ReactFlow nodes={flow.nodes} edges={flow.edges}
    fitView fitViewOptions={{ padding: 0.2 }} nodesDraggable={false} elementsSelectable={false}>
    <Background gap={18} color="#dce7f1" /><Controls />
  </ReactFlow></div>
}
