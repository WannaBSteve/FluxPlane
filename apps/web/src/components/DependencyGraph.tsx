import { Fragment, useMemo } from 'react'
import { Background, Controls, Position, ReactFlow, type Edge, type Node } from '@xyflow/react'
import type { Dataset, Feature } from '@/lib/api'

function nodeName(name: string) {
  return name.split('_').map((part, index) =>
    <Fragment key={index}>{index > 0 && <>_<wbr /></>}{part}</Fragment>)
}

export default function DependencyGraph({ datasets, features, onOpen }: { datasets: Dataset[]; features: Feature[]; onOpen?: (name: string) => void }) {
  const flow = useMemo(() => {
    const nodes: Node[] = datasets.map((dataset, index) => ({
      id: dataset.name,
      position: { x: 30, y: index * 86 + 30 },
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
      data: { label: <div className="flow-node dependency-node" title={dataset.name}><strong>{nodeName(dataset.name)}</strong><span>数据集 · {dataset.active_version_id ? '已发布' : '待同步'}</span></div> },
      style: { border: 'none', background: 'none', padding: 0 },
    }))
    const edges: Edge[] = []
    features.forEach((feature, index) => {
      nodes.push({
        id: feature.name,
        position: { x: 310, y: index * 100 + 50 },
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
        data: { label: <div className="flow-node dependency-node feature" title={feature.name}><strong>{nodeName(feature.name)}</strong><span>特征 · {feature.stale ? '待更新' : feature.active_snapshot_id ? '已发布' : '待刷新'}</span></div> },
        style: { border: 'none', background: 'none', padding: 0 },
      })
      feature.dependencies.forEach(name => edges.push({
        id: `${name}-${feature.name}`, source: name, target: feature.name,
        animated: feature.stale || !feature.active_snapshot_id,
        style: { stroke: '#88a4d9', strokeWidth: 1.7 },
      }))
    })
    return { nodes, edges }
  }, [datasets, features])

  return <div className="graph-wrap">{flow.nodes.length ?
    <ReactFlow nodes={flow.nodes} edges={flow.edges} fitView fitViewOptions={{ padding: 0.15 }}
      nodesDraggable={false} elementsSelectable onNodeClick={(_, node) => onOpen?.(node.id)}>
      <Background gap={18} color="#dce7f1" /><Controls />
    </ReactFlow> : <div className="empty">接入数据后，依赖关系会显示在这里。</div>}
  </div>
}
