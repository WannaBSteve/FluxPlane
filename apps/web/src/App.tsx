import { lazy, Suspense, useCallback, useEffect, useState } from 'react'
import type { FormEvent, ReactNode } from 'react'
import {
  Activity, ArrowRight, Boxes, CheckCircle2, CircleAlert, Clock3, Code2, Database,
  FileStack, GitBranch, Layers3, LayoutDashboard, Moon, Plus, RefreshCcw,
  KeyRound, Search, Sun, Workflow, X,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { api, post, short, moment, type Dataset, type Feature,
  watchEvents, type Job, type Materialization, type Metrics, type Release, type Role, type Source, type SourceKind, type StorageSummary, type Workflow as WorkflowRecord } from '@/lib/api'
import type { DatasetLayout } from '@/lib/api'

import DataExplorer, { isFeatureItem } from '@/components/DataExplorer'
import AnalysisWorkspace from '@/components/AnalysisWorkspace'
import '@/styles/exploration.css'

type Page = 'overview' | 'sources' | 'features' | 'tasks' | 'query' | 'versions'
type Modal = 'source' | 'dataset' | 'feature' | 'workflow' | 'layout' | null

const ExecutionChart = lazy(() => import('@/components/ExecutionChart'))
const DependencyGraph = lazy(() => import('@/components/DependencyGraph'))
const WorkflowGraph = lazy(() => import('@/components/WorkflowGraph'))

const pages: { id: Page; label: string; icon: typeof Database; hint: string }[] = [
  { id: 'overview', label: '系统总览', icon: LayoutDashboard, hint: '运行状态与平台概况' },
  { id: 'sources', label: '数据目录', icon: Database, hint: '数据源、数据集和同步' },
  { id: 'features', label: '特征中心', icon: GitBranch, hint: '特征定义与依赖更新' },
  { id: 'tasks', label: '任务中心', icon: Workflow, hint: '执行队列与运行结果' },
  { id: 'query', label: '分析工作台', icon: Code2, hint: '用自然语言提问，查看操作计划与分析结果' },
  { id: 'versions', label: '版本与存储', icon: Layers3, hint: '数据与特征版本' },
]

function pageFromUrl(): Page {
  const view = new URLSearchParams(window.location.search).get('view')
  return pages.some(item => item.id === view) ? view as Page : 'overview'
}

function statusTone(status: string) {
  if (status === 'succeeded') return 'positive'
  if (status === 'failed') return 'negative'
  return 'pending'
}

function statusText(status: string) {
  return ({ pending: '等待中', running: '运行中', succeeded: '已完成', failed: '失败',
    cancelling: '取消中', cancelled: '已取消' } as Record<string, string>)[status] ?? status
}

function jobKindText(kind: string) {
  return ({ sync: '数据同步', refresh: '特征刷新', query: '分析查询',
    materialize: '查询物化', organize: '分区整理' } as Record<string, string>)[kind] ?? kind
}

function bytes(value: number) {
  return value >= 1024 * 1024 ? `${(value / (1024 * 1024)).toFixed(1)} MiB` : `${(value / 1024).toFixed(1)} KiB`
}

function ShellSection({ title, subtitle, action, children }: { title: string; subtitle?: string; action?: ReactNode; children: ReactNode }) {
  return <section className="surface">
    <div className="surface-head"><div><div className="surface-title">{title}</div>{subtitle && <div className="surface-sub">{subtitle}</div>}</div>{action}</div>
    {children}
  </section>
}

function MiniMetric({ title, value, note, icon: Icon }: { title: string; value: string | number; note: string; icon: typeof Database }) {
  return <div className="metric">
    <div className="flex items-center justify-between"><span className="metric-label">{title}</span><Icon size={17} className="text-[#8da0b7]" /></div>
    <div className="metric-value">{value}</div><div className="metric-foot"><Activity size={12} />{note}</div>
  </div>
}

function Dialog({ title, onClose, onSave, children, saving }: { title: string; onClose: () => void; onSave: (event: FormEvent) => void; children: ReactNode; saving: boolean }) {
  return <div className="dialog-backdrop" onMouseDown={onClose}>
    <form className="dialog" onMouseDown={event => event.stopPropagation()} onSubmit={onSave}>
      <div className="dialog-head">{title}<button type="button" onClick={onClose} aria-label="关闭"><X size={18} /></button></div>
      <div className="dialog-body form-grid">{children}</div>
      <div className="dialog-foot"><Button type="button" variant="outline" onClick={onClose}>取消</Button><Button disabled={saving} type="submit">{saving ? '保存中…' : '保存'}</Button></div>
    </form>
  </div>
}

export function App() {
  const [page, setPage] = useState<Page>(pageFromUrl)
  const [metrics, setMetrics] = useState<Metrics>({ sources: 0, datasets: 0, features: 0,
    jobs: 0, running_jobs: 0, pools: [], workload: [], resources: {
      cpu_budget_millicores: 0, cpu_reserved_millicores: 0,
      memory_budget_mb: 0, memory_reserved_mb: 0,
    } })
  const [sources, setSources] = useState<Source[]>([])
  const [datasets, setDatasets] = useState<Dataset[]>([])
  const [features, setFeatures] = useState<Feature[]>([])
  const [jobs, setJobs] = useState<Job[]>([])
  const [workflows, setWorkflows] = useState<WorkflowRecord[]>([])
  const [selectedWorkflowId, setSelectedWorkflowId] = useState<string | null>(null)
  const [taskPage, setTaskPage] = useState(0)
  const [releases, setReleases] = useState<Release[]>([])
  const [showAllReleases, setShowAllReleases] = useState(false)
  const [storage, setStorage] = useState<StorageSummary | null>(null)
  const [materializations, setMaterializations] = useState<Materialization[]>([])
  const [layouts, setLayouts] = useState<DatasetLayout[]>([])
  const [error, setError] = useState('')
  const [toast, setToast] = useState('')
  const [modal, setModal] = useState<Modal>(null)
  const [saving, setSaving] = useState(false)
  const [dark, setDark] = useState(() => localStorage.getItem('fluxplane-theme') === 'dark')
  const [apiKey, setApiKey] = useState(() => sessionStorage.getItem('fluxplane-api-key') ?? '')
  const [activeKey, setActiveKey] = useState(() => sessionStorage.getItem('fluxplane-api-key') ?? '')
  const [role, setRole] = useState<Role | null>(null)
  const [live, setLive] = useState(false)
  const [keyOpen, setKeyOpen] = useState(false)
  const [searchOpen, setSearchOpen] = useState(false)
  const [searchQuery, setSearchQuery] = useState('')
  const [filter, setFilter] = useState(() => new URLSearchParams(window.location.search).get('q') ?? '')
  const [selectedJob, setSelectedJob] = useState<Job | null>(null)
  const [analysisRelation, setAnalysisRelation] = useState('')
  const [sourceForm, setSourceForm] = useState({ name: '', kind: 'file' as SourceKind, location: '',
    primary_key: '', updated_at: '', deleted_column: '', field_map: '',
    records_path: '', next_path: '' })
  const [datasetForm, setDatasetForm] = useState({ name: '', source_id: '', source_object: '' })
  const [layoutForm, setLayoutForm] = useState({ dataset_id: '', partition_key: '' })
  const [featureForm, setFeatureForm] = useState({ name: '', dependencies: '', entity_keys: '', sql: '',
    mode: 'sql', measure: '', event_time: '', window_seconds: '',
    sum_alias: 'total', count_alias: 'row_count', refresh_interval_seconds: '',
    text_column: 'text', terms: '', output_column: 'match_count' })
  const [windowRules, setWindowRules] = useState<Record<string, { enabled: boolean; event_time: string; window_seconds: string }>>({})
  const [workflowForm, setWorkflowForm] = useState({ name: '', datasetIds: [] as string[],
    featureIds: [] as string[], sql: '' })

  const refresh = useCallback(async () => {
    const results = await Promise.allSettled([
      api<{ role: Role }>('/auth/me'),
      api<Metrics>('/system/metrics'), api<Source[]>('/sources'), api<Dataset[]>('/datasets'),
      api<Feature[]>('/features'), api<Job[]>('/runs?limit=60'), api<Release[]>('/releases?limit=40'),
      api<StorageSummary>('/storage/summary'), api<WorkflowRecord[]>('/workflows?limit=20'),
      api<Materialization[]>('/materializations?limit=30'),
      api<DatasetLayout[]>('/layouts?limit=50'),
    ])
    const [identity, m, s, d, f, j, r, z, w, a, l] = results
    setRole(identity.status === 'fulfilled' ? identity.value.role : null)
    if (m.status === 'fulfilled') setMetrics(m.value)
    if (s.status === 'fulfilled') setSources(s.value)
    if (d.status === 'fulfilled') setDatasets(d.value)
    if (f.status === 'fulfilled') setFeatures(f.value)
    if (j.status === 'fulfilled') setJobs(j.value)
    if (r.status === 'fulfilled') setReleases(r.value)
    if (z.status === 'fulfilled') setStorage(z.value)
    if (w.status === 'fulfilled') setWorkflows(w.value)
    if (a.status === 'fulfilled') setMaterializations(a.value)
    if (l.status === 'fulfilled') setLayouts(l.value)
    const failed = results.find(value => value.status === 'rejected')
    if (failed?.status === 'rejected') setError(String(failed.reason))
    else setError('')
  }, [])

  useEffect(() => {
    const controller = new AbortController()
    setRole(null)
    let authenticated = false
    let refreshTimer: ReturnType<typeof setTimeout> | undefined
    const scheduleRefresh = () => {
      if (!refreshTimer) refreshTimer = setTimeout(() => {
        refreshTimer = undefined
        void refresh()
      }, 150)
    }
    const waitToReconnect = () => new Promise<void>(resolve => {
      const done = () => {
        clearTimeout(timer)
        controller.signal.removeEventListener('abort', done)
        resolve()
      }
      const timer = setTimeout(done, 3000)
      controller.signal.addEventListener('abort', done, { once: true })
    })
    const connect = async () => {
      let cursor = 0
      try {
        cursor = (await api<{ cursor: number }>('/events/cursor')).cursor
        authenticated = true
      } catch (cause) {
        if (cause instanceof Error && cause.message === 'invalid API key') {
          setError('请设置正确的实例访问密钥')
          setKeyOpen(true)
          return
        }
      }
      if (controller.signal.aborted) return
      await refresh()
      while (!controller.signal.aborted) {
        try {
          await watchEvents(cursor, controller.signal, event => {
            cursor = event.id
            scheduleRefresh()
          }, () => { authenticated = true; setLive(true) })
        } catch (cause) {
          if (cause instanceof Error && cause.message === '事件流连接失败 (401)') {
            authenticated = false
            setLive(false)
            setError('请设置正确的实例访问密钥')
            setKeyOpen(true)
            return
          }
        }
        if (controller.signal.aborted) break
        setLive(false)
        await waitToReconnect()
      }
    }
    void connect()
    const fallback = setInterval(() => { if (authenticated) void refresh() }, 30_000)
    const onVisibility = () => { if (!document.hidden) void refresh() }
    document.addEventListener('visibilitychange', onVisibility)
    return () => {
      controller.abort()
      clearTimeout(refreshTimer)
      clearInterval(fallback)
      document.removeEventListener('visibilitychange', onVisibility)
    }
  }, [refresh, activeKey])
  useEffect(() => { document.documentElement.classList.toggle('dark', dark); localStorage.setItem('fluxplane-theme', dark ? 'dark' : 'light') }, [dark])
  useEffect(() => { if (!toast) return; const timer = setTimeout(() => setToast(''), 4000); return () => clearTimeout(timer) }, [toast])
  useEffect(() => {
    const url = new URL(window.location.href)
    if (page === 'overview') url.searchParams.delete('view')
    else url.searchParams.set('view', page)
    if (filter) url.searchParams.set('q', filter)
    else url.searchParams.delete('q')
    window.history.replaceState(null, '', url)
  }, [page, filter])
  useEffect(() => {
    const onPopState = () => {
      setPage(pageFromUrl())
      setFilter(new URLSearchParams(window.location.search).get('q') ?? '')
      setTaskPage(0)
    }
    const onKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault()
        setSearchOpen(value => !value)
      }
      if (event.key === 'Escape') setSearchOpen(false)
    }
    window.addEventListener('popstate', onPopState)
    window.addEventListener('keydown', onKeyDown)
    return () => {
      window.removeEventListener('popstate', onPopState)
      window.removeEventListener('keydown', onKeyDown)
    }
  }, [])

  const navigate = (next: Page, query = '') => {
    const url = new URL(window.location.href)
    if (next === 'overview') url.searchParams.delete('view')
    else url.searchParams.set('view', next)
    if (query) url.searchParams.set('q', query)
    else url.searchParams.delete('q')
    window.history.pushState(null, '', url)
    setPage(next)
    setFilter(query)
    setTaskPage(0)
    setSearchOpen(false)
  }

  const run = async (operation: () => Promise<unknown>, message: string) => {
    setSaving(true)
    try { await operation(); setModal(null); setError(''); setToast(message); await refresh() }
    catch (cause) { setError(cause instanceof Error ? cause.message : String(cause)) }
    finally { setSaving(false) }
  }

  const saveSource = (event: FormEvent) => {
    event.preventDefault()
    const fieldMap: Record<string, string> = {}
    if (sourceForm.kind === 'http' && sourceForm.field_map.trim()) {
      for (const entry of sourceForm.field_map.split(',')) {
        const fields = entry.split(':').map(value => value.trim())
        if (fields.length !== 2 || !fields[0] || !fields[1] || fieldMap[fields[0]]) {
          setError('字段映射格式应为“来源字段:目录字段”，多个映射用逗号分隔')
          return
        }
        fieldMap[fields[0]] = fields[1]
      }
    }
    const config = sourceForm.kind === 'sqlite' ? { path: sourceForm.location } :
      sourceForm.kind === 'postgres' ? { database_url_env: sourceForm.location,
        ...(sourceForm.primary_key || sourceForm.updated_at ? {
          primary_key: sourceForm.primary_key, updated_at: sourceForm.updated_at,
          ...(sourceForm.deleted_column ? { deleted_column: sourceForm.deleted_column } : {}),
        } : {}),
      } :
      sourceForm.kind === 'http' ? { url: sourceForm.location,
        ...(Object.keys(fieldMap).length ? { field_map: fieldMap } : {}),
        ...(sourceForm.records_path ? { records_path: sourceForm.records_path.trim() } : {}),
        ...(sourceForm.next_path ? { next_path: sourceForm.next_path.trim() } : {}),
        ...(sourceForm.primary_key || sourceForm.updated_at ? {
          primary_key: sourceForm.primary_key, updated_at: sourceForm.updated_at,
          ...(sourceForm.deleted_column ? { deleted_column: sourceForm.deleted_column } : {}),
        } : {}),
      } : {}
    void run(() => post('/sources', { name: sourceForm.name, kind: sourceForm.kind, config }), '数据源已注册')
  }

  const saveDataset = (event: FormEvent) => {
    event.preventDefault()
    void run(() => post('/datasets', datasetForm), '数据集已创建')
  }

  const saveLayout = (event: FormEvent) => {
    event.preventDefault()
    void run(() => post(`/datasets/${layoutForm.dataset_id}/layouts`,
      { partition_key: layoutForm.partition_key }), '已提交分区整理任务')
  }

  const saveFeature = (event: FormEvent) => {
    event.preventDefault()
    const dependencies = featureForm.dependencies.split(',').map(x => x.trim()).filter(Boolean)
    const value = { name: featureForm.name,
      sql: ['sql', 'window_sql'].includes(featureForm.mode) ? featureForm.sql : null,
      dependencies,
      entity_keys: featureForm.entity_keys.split(',').map(x => x.trim()).filter(Boolean),
      refresh_interval_seconds: featureForm.mode === 'sql' && featureForm.refresh_interval_seconds
        ? Number(featureForm.refresh_interval_seconds) : null,
      maintenance: featureForm.mode === 'sql' ? null : featureForm.mode === 'python' ? {
        mode: 'python', operator: 'text_match_count', version: '1',
        text_column: featureForm.text_column,
        terms: featureForm.terms.split(',').map(x => x.trim()).filter(Boolean),
        output_column: featureForm.output_column,
      } : featureForm.mode === 'window_sql' ? {
        mode: 'window_sql', windows: Object.fromEntries(Object.entries(windowRules)
          .filter(([name, rule]) => rule.enabled && dependencies.includes(name))
          .map(([name, rule]) => [name, { event_time: rule.event_time,
            window_seconds: Number(rule.window_seconds) }])),
      } : {
        mode: featureForm.mode, measure: featureForm.measure,
        sum_alias: featureForm.sum_alias, count_alias: featureForm.count_alias,
        ...(featureForm.mode === 'rolling_sum' ? {
          event_time: featureForm.event_time, window_seconds: Number(featureForm.window_seconds),
        } : {}),
      } }
    void run(() => post('/features', value), '特征已创建')
  }

  const saveWorkflow = (event: FormEvent) => {
    event.preventDefault()
    const steps = [
      ...workflowForm.datasetIds.map((id, index) => ({ key: `sync_${index + 1}`, kind: 'sync', target_id: id })),
      ...workflowForm.featureIds.map((id, index) => ({ key: `refresh_${index + 1}`, kind: 'refresh', target_id: id })),
      ...(workflowForm.sql.trim() ? [{ key: 'report', kind: 'query', sql: workflowForm.sql.trim() }] : []),
    ]
    if (!steps.length) { setError('至少选择一个同步、刷新或查询步骤'); return }
    void run(async () => {
      const created = await post<WorkflowRecord>('/workflows', { name: workflowForm.name, steps })
      setSelectedWorkflowId(created.id)
    }, '执行计划已提交')
  }

  const sync = (dataset: Dataset) => void run(() => post(`/datasets/${dataset.id}/sync`), `已提交 ${dataset.name} 同步`)
  const updateFeature = (feature: Feature) => void run(() => post(`/features/${feature.id}/refresh`), `已提交 ${feature.name} 刷新`)
  const restore = (release: Release) => {
    if (!window.confirm(`恢复发布版本 ${short(release.id)}？系统会创建一条新的发布记录。`)) return
    void run(() => post(`/releases/${release.id}/restore`), '版本组合已恢复')
  }

  const match = (value: string) => value.toLowerCase().includes(filter.toLowerCase())
  const visibleJobs = jobs.filter(x => match(x.kind) || match(x.status) || match(x.id))
  const taskPageCount = Math.max(1, Math.ceil(visibleJobs.length / 12))
  const currentTaskPage = Math.min(taskPage, taskPageCount - 1)
  const pagedJobs = visibleJobs.slice(currentTaskPage * 12, (currentTaskPage + 1) * 12)
  const selectedWorkflow = workflows.find(x => x.id === selectedWorkflowId) ?? workflows[0]
  const workflowTargets = Object.fromEntries([
    ...datasets.map(item => [item.id, item.name]),
    ...features.map(item => [item.id, item.name]),
  ])
  const globalHits = [
    ...datasets.map(item => ({ id: item.id, title: item.name,
      detail: `数据集 · ${item.source_object}`, page: 'sources' as Page, query: item.name })),
    ...features.map(item => ({ id: item.id, title: item.name,
      detail: `特征 · ${item.dependencies.join('、')}`, page: 'features' as Page, query: item.name })),
    ...jobs.map(item => ({ id: item.id, title: `任务 ${short(item.id)}`,
      detail: `${jobKindText(item.kind)} · ${statusText(item.status)}`,
      page: 'tasks' as Page, query: item.id })),
  ].filter(item => `${item.title} ${item.detail} ${item.id}`.toLowerCase().includes(searchQuery.trim().toLowerCase())).slice(0, 12)
  const activePage = pages.find(x => x.id === page)!
  const canOperate = role === 'admin' || role === 'operator'
  const canAdmin = role === 'admin'

  return <div className="shell">
    <aside className="sidebar">
      <div className="brand"><div className="brand-icon"><Boxes size={20} /></div><div><div className="brand-name">FluxPlane</div><div className="brand-sub">MULTIMODAL FEATURE FUSION</div></div></div>
      <div className="nav-label">WORKSPACE</div>
      <nav className="nav-wrap" aria-label="主导航">{pages.map(item => <button key={item.id} className={`nav-item ${page === item.id ? 'active' : ''}`} onClick={() => navigate(item.id)}><item.icon size={17} strokeWidth={1.8} />{item.label}</button>)}</nav>
      <div className="sidebar-bottom"><div className="system-pill"><span className="status-dot" />{error ? '服务连接待恢复' : '系统运行中'}</div><div className="mt-3 text-[11px] text-[#94a2b5]">多源数据 · 动态调度 · 持续更新</div></div>
    </aside>
    <div className="main-area">
      <header className="topbar"><div className="breadcrumbs"><span>工作空间</span><ArrowRight size={12} /><strong>{activePage.label}</strong></div><div className="top-actions"><span className="subtle hidden md:block">FluxPlane / 本地实例</span><span className="subtle hidden sm:block">{role === 'admin' ? '管理员' : role === 'operator' ? '操作员' : role === 'viewer' ? '只读' : '未验证'}</span><button onClick={() => { setSearchQuery(''); setSearchOpen(true) }} aria-label="全局搜索" title="全局搜索 Ctrl+K" className="rounded-lg border border-[var(--border)] p-2"><Search size={17} /></button><button onClick={() => setKeyOpen(true)} aria-label="设置访问密钥" className="rounded-lg border border-[var(--border)] p-2"><KeyRound size={17} /></button><button onClick={() => setDark(!dark)} aria-label="切换主题" className="rounded-lg border border-[var(--border)] p-2">{dark ? <Sun size={17} /> : <Moon size={17} />}</button><div className="grid h-8 w-8 place-items-center rounded-full bg-[#dbe6ff] text-xs font-bold text-[#2f58bd]">FP</div></div></header>
      <main className="content">
        <div className="page-heading"><div><h1 className="page-title">{activePage.label}</h1><p className="page-subtitle">{activePage.hint}</p></div><div className="flex items-center gap-3"><span className="subtle hidden sm:inline" role="status"><span className={live ? 'positive' : 'pending'}>●</span> {live ? '实时更新' : '定时更新'}</span><Button variant="outline" onClick={() => void refresh()}><RefreshCcw size={15} />刷新状态</Button></div></div>
        {error && <div role="alert" className="notice"><CircleAlert size={14} className="mr-2 inline" />{error}</div>}

        {page === 'overview' && <>
          <div className="metrics"><MiniMetric title="数据源" value={metrics.sources} note="已注册连接" icon={Database} /><MiniMetric title="数据集" value={metrics.datasets} note="统一数据目录" icon={FileStack} /><MiniMetric title="特征定义" value={metrics.features} note="持续维护" icon={GitBranch} /><MiniMetric title="运行任务" value={metrics.jobs} note={`${metrics.running_jobs} 个正在执行`} icon={Activity} /></div>
          <div className="section-grid">
            <ShellSection title="最近任务" subtitle="同步、刷新与分析任务的真实运行状态" action={<button className="text-link" onClick={() => navigate('tasks')}>查看全部 →</button>}>
              {jobs.length ? <Table><TableHeader><TableRow><TableHead>任务</TableHead><TableHead>类型</TableHead><TableHead>状态</TableHead><TableHead>创建时间</TableHead></TableRow></TableHeader><TableBody>{jobs.slice(0, 6).map(job => <TableRow key={job.id}><TableCell className="code">{short(job.id)}</TableCell><TableCell>{jobKindText(job.kind)}</TableCell><TableCell><span className={statusTone(job.status)}>● {statusText(job.status)}</span></TableCell><TableCell className="subtle">{moment(job.created_at)}</TableCell></TableRow>)}</TableBody></Table> : <div className="empty">还没有任务。先在数据目录注册来源和数据集。</div>}
            </ShellSection>
            <ShellSection title="执行分布" subtitle="根据当前任务列表实时统计"><div className="surface-body"><Suspense fallback={<div className="chart-wrap" />}><ExecutionChart jobs={jobs} /></Suspense></div></ShellSection>
          </div>
          <div className="mt-5 section-grid"><ShellSection title="数据状态" subtitle="最新可用的已发布数据版本"><div className="surface-body card-list">{datasets.slice(0, 4).map(dataset => <div className="record" key={dataset.id}><span className="record-icon"><Database size={16} /></span><div className="record-main"><button className="record-title text-left" onClick={() => navigate('sources', dataset.name)}>{dataset.name}</button><div className="record-sub">{dataset.source_object}</div></div><span className={dataset.active_version_id ? 'positive' : 'pending'}>{dataset.active_version_id ? '已发布' : '待同步'}</span></div>)}{!datasets.length && <div className="empty">暂无数据集</div>}</div></ShellSection><ShellSection title="特征更新" subtitle="依赖数据变化自动产生刷新任务"><div className="surface-body card-list">{features.slice(0, 4).map(feature => <div className="record" key={feature.id}><span className="record-icon"><GitBranch size={16} /></span><div className="record-main"><button className="record-title text-left" onClick={() => navigate('features', feature.name)}>{feature.name}</button><div className="record-sub">{feature.dependencies.join(' + ')}</div></div><span className={feature.active_snapshot_id ? 'positive' : 'pending'}>{feature.active_snapshot_id ? '已发布' : '待刷新'}</span></div>)}{!features.length && <div className="empty">暂无特征定义</div>}</div></ShellSection></div>
          <div className="mt-5"><ShellSection title="执行耗时" subtitle="由已完成任务的实际耗时汇总"><div className="surface-body workload-strip">{metrics.workload.length ? metrics.workload.map(item => <div className="workload-item runtime-item" key={item.lane}><span className="subtle">{item.lane === 'query' ? '交互执行池' : '后台执行池'}</span><strong>{item.avg_duration_ms.toLocaleString()} ms</strong>{item.avg_cpu_ms !== null && <span className="subtle">CPU {item.avg_cpu_ms.toLocaleString()} ms · 峰值 RSS 均值 {item.avg_peak_rss_mb?.toLocaleString() ?? "—"} MiB（{item.resource_samples} 次实测）</span>}<span className="subtle">近 1 小时完成 {item.completed} 次</span></div>) : <div className="empty">运行任务后会显示实际耗时。</div>}</div></ShellSection></div>
        </>}

        {page === 'sources' && <>
          <div className="toolbar"><div className="subtle">表、文档与接口数据，按来源整理</div><div className="toolbar-actions">{canAdmin && <Button variant="outline" onClick={() => setModal('source')}><Plus size={14} />注册数据源</Button>}{canAdmin && <Button onClick={() => { setDatasetForm(x => ({ ...x, source_id: sources[0]?.id ?? '' })); setModal('dataset') }} disabled={!sources.length}><Plus size={14} />创建数据集</Button>}</div></div>
          <DataExplorer key={activeKey + 'datasets'} datasets={datasets} features={features} sources={sources} filter={filter} onAnalyze={name => { setAnalysisRelation(name); navigate('query') }} onSync={canOperate ? item => isFeatureItem(item) ? updateFeature(item) : sync(item) : undefined} onLayout={canAdmin ? item => { setLayoutForm({ dataset_id: item.id, partition_key: '' }); setModal('layout') } : undefined} />
          {!!layouts.length && <details className="surface advanced-section"><summary>分区运行记录 · {layouts.length}</summary><div className="surface-body">{layouts.map(l => <div className="record" key={l.id}><strong>{l.dataset_name}</strong><span>{l.partition_key} · {l.partition_count ?? '—'} 个分区 · 查询使用 {l.query_count} 次</span><span>{l.current ? '当前版本' : '历史版本'}</span></div>)}</div></details>}
        </>}
        {page === 'features' && <>
          <div className="toolbar"><div className="subtle">查看融合结果、输入关系和更新记录</div>{canAdmin && <Button onClick={() => setModal('feature')} disabled={!datasets.length}><Plus size={14} />新建特征</Button>}</div>
          <DataExplorer key={activeKey + 'features'} mode="features" datasets={datasets} features={features} sources={sources} filter={filter} onAnalyze={name => { setAnalysisRelation(name); navigate('query') }} onSync={canOperate ? item => isFeatureItem(item) ? updateFeature(item) : sync(item) : undefined} />
          <details className="surface advanced-section"><summary>查看全部依赖关系</summary><Suspense fallback={<div className="empty">正在加载…</div>}><DependencyGraph datasets={datasets} features={features} onOpen={name => navigate(datasets.some(d => d.name === name) ? 'sources' : 'features', name)} /></Suspense></details>
        </>}

        {page === 'tasks' && <div className="mb-5"><ShellSection title="执行计划" subtitle="按输入依赖安排同步、刷新与查询" action={canOperate && <Button onClick={() => setModal('workflow')}><Plus size={14} />新建计划</Button>}><div className="surface-body"><div className="flex flex-wrap gap-2 mb-4">{workflows.map(item => <button key={item.id} className={`rounded-lg border px-3 py-2 text-left text-sm ${selectedWorkflow?.id === item.id ? 'border-blue-400 bg-blue-50 dark:bg-slate-800' : 'border-[var(--border)]'}`} onClick={() => setSelectedWorkflowId(item.id)}><strong>{item.name}</strong><span className={`ml-3 ${statusTone(item.status)}`}>● {statusText(item.status)}</span></button>)}</div>{selectedWorkflow ? <><div className="workflow-graph-scroll"><Suspense fallback={<div className="graph-wrap workflow-graph" />}><WorkflowGraph workflow={selectedWorkflow} targets={workflowTargets} /></Suspense></div><div className="subtle mt-3">{selectedWorkflow.jobs.length} 个步骤 · {selectedWorkflow.jobs.filter(job => job.status === 'succeeded').length} 个已完成<span className="workflow-scroll-hint"> · 横向滑动查看全图</span></div></> : <div className="empty">选择数据集或特征，创建第一条执行计划。</div>}</div></ShellSection></div>}
        {page === 'tasks' && <><div className="toolbar"><div className="toolbar-search"><Input placeholder="搜索任务、状态或编号" value={filter} onChange={event => { setFilter(event.target.value); setTaskPage(0) }} /></div><div className="subtle">最近 {jobs.length} 条 · 累计 {metrics.jobs} 条</div></div><div className="pool-strip">{metrics.pools.map(pool => <div className="pool-card" key={pool.lane}><span className="subtle">{pool.lane === 'query' ? '交互执行池' : '后台执行池'}</span><strong>{pool.running} / {pool.max_capacity}</strong><span className="subtle">保留 {pool.capacity} · 等待 {pool.pending}</span></div>)}{metrics.resources && <><div className="pool-card"><span className="subtle">CPU 预留</span><strong>{metrics.resources.cpu_reserved_millicores} / {metrics.resources.cpu_budget_millicores} mCPU</strong></div><div className="pool-card"><span className="subtle">内存预留</span><strong>{metrics.resources.memory_reserved_mb} / {metrics.resources.memory_budget_mb} MiB</strong></div></>}</div><ShellSection title="任务队列" subtitle="任务状态由执行进程实时更新"><Table className="task-table-desktop"><TableHeader><TableRow><TableHead>编号</TableHead><TableHead>操作</TableHead><TableHead>执行池</TableHead><TableHead>状态</TableHead><TableHead>尝试</TableHead><TableHead>提交时间</TableHead><TableHead /></TableRow></TableHeader><TableBody>{pagedJobs.map(job => <TableRow key={job.id}><TableCell className="code">{short(job.id)}</TableCell><TableCell className="font-medium">{jobKindText(job.kind)}</TableCell><TableCell>{job.lane === 'query' ? '交互' : '后台'}</TableCell><TableCell><span className={statusTone(job.status)}>● {statusText(job.status)}</span></TableCell><TableCell>{job.attempt}</TableCell><TableCell className="subtle">{moment(job.created_at)}</TableCell><TableCell><button className="text-link" onClick={() => setSelectedJob(job)}>详情</button></TableCell></TableRow>)}</TableBody></Table><div className="task-mobile-list">{pagedJobs.map(job => <div className="task-mobile-card" key={job.id}><div className="flex items-center justify-between gap-2"><span className="font-semibold">{jobKindText(job.kind)}</span><span className={statusTone(job.status)}>● {statusText(job.status)}</span></div><div className="subtle mt-2">{short(job.id)} · {job.lane === 'query' ? '交互' : '后台'} · {moment(job.created_at)}</div><div className="flex items-center justify-between mt-3"><span className="subtle">尝试 {job.attempt} 次</span><button className="text-link" onClick={() => setSelectedJob(job)}>查看详情 →</button></div></div>)}</div>{!visibleJobs.length && <div className="empty">暂无匹配任务</div>}{visibleJobs.length > 12 && <div className="task-pagination"><span>第 {currentTaskPage + 1} / {taskPageCount} 页 · {visibleJobs.length} 条匹配</span><div><Button variant="outline" disabled={currentTaskPage === 0} onClick={() => setTaskPage(currentTaskPage - 1)}>上一页</Button><Button variant="outline" disabled={currentTaskPage >= taskPageCount - 1} onClick={() => setTaskPage(currentTaskPage + 1)}>下一页</Button></div></div>}</ShellSection></>}

        {page === 'query' && <AnalysisWorkspace key={activeKey} datasets={datasets} features={features} jobs={jobs} initialRelation={analysisRelation} canOperate={canOperate} onRefresh={refresh} />}

        {page === 'versions' && <><div className="section-grid"><ShellSection title="数据版本" subtitle="查询固定绑定所使用的版本"><div className="surface-body card-list">{datasets.map(dataset => <div className="record" key={dataset.id}><span className="record-icon"><Database size={16} /></span><div className="record-main"><div className="record-title">{dataset.name}</div><div className="record-sub">{dataset.source_object}</div></div><span className="code subtle">{short(dataset.active_version_id)}</span></div>)}{!datasets.length && <div className="empty">暂无数据版本</div>}</div></ShellSection><ShellSection title="特征快照" subtitle="每个快照记录其输入版本"><div className="surface-body card-list">{features.map(feature => <div className="record" key={feature.id}><span className="record-icon"><GitBranch size={16} /></span><div className="record-main"><div className="record-title">{feature.name}</div><div className="record-sub">{Object.entries(feature.input_versions).map(([name, id]) => `${name} ${short(id)}`).join(' · ') || '尚未发布'}</div></div><span className="code subtle">{short(feature.active_snapshot_id)}</span></div>)}{!features.length && <div className="empty">暂无特征快照</div>}</div></ShellSection></div>
          <div className="mt-5"><ShellSection title="查询物化" subtitle="重复查询在相同输入版本上复用已验证结果"><div className="surface-body card-list">{materializations.slice(0, 8).map(item => <div className="record" key={item.id}><span className="record-icon"><Layers3 size={16} /></span><div className="record-main"><div className="record-title code" title={item.sql}>{item.sql}</div><div className="record-sub">{Object.keys(item.input_versions).join('、')} · 观察 {item.observed_queries} 次 · 构建 {item.build_duration_ms?.toFixed(1) ?? '—'} ms · 复用 {item.reuse_count} 次{item.artifact_bytes != null ? ` · ${bytes(item.artifact_bytes)}` : ''}{item.estimated_net_ms != null ? ` · 估计净收益 ${item.estimated_net_ms.toFixed(1)} ms` : ''}</div></div><span className={item.status === 'active' ? 'positive' : item.status === 'rejected' ? 'negative' : 'pending'} title={item.error ?? undefined}>{item.status === 'active' ? item.current ? '可复用' : '非当前输入' : item.status === 'building' ? '构建中' : item.status === 'rejected' ? '未启用' : item.status === 'retired' ? '已淘汰' : '观察中'}</span></div>)}{!materializations.length && <div className="empty">重复的稳定查询出现后，系统会在后台构建可复用结果。</div>}</div></ShellSection></div><div className="mt-5"><ShellSection title="存储占用" subtitle="孤儿产物统计仅包含超过一小时、且未被引用的文件"><div className="surface-body workload-strip"><div className="workload-item"><span className="subtle">产物总量</span><strong>{bytes(storage?.artifact_bytes ?? 0)}</strong><span className="subtle">{storage?.artifact_count ?? 0} 个文件</span></div><div className="workload-item"><span className="subtle">可清理孤儿产物</span><strong>{bytes(storage?.orphan_bytes ?? 0)}</strong><span className="subtle">{storage?.orphan_count ?? 0} 个文件</span></div></div></ShellSection></div><div className="mt-5"><ShellSection title="发布历史" subtitle="每次数据或特征发布都固定一组版本；恢复会生成新的发布记录" action={releases.length > 8 && <Button variant="outline" onClick={() => setShowAllReleases(value => !value)}>{showAllReleases ? "收起" : `查看全部 ${releases.length} 条`}</Button>}><div className="overflow-x-auto"><Table className="min-w-[640px]"><TableHeader><TableRow><TableHead>Release</TableHead><TableHead>内容</TableHead><TableHead>来源</TableHead><TableHead>时间</TableHead><TableHead className="text-right">操作</TableHead></TableRow></TableHeader><TableBody>{(showAllReleases ? releases : releases.slice(0, 8)).map(release => <TableRow key={release.id}><TableCell className="code">{short(release.id)}</TableCell><TableCell>{Object.keys(release.dataset_versions).length} 个数据集 · {Object.keys(release.feature_snapshots).length} 个特征</TableCell><TableCell className="subtle">{release.restored_from_id ? `恢复自 ${short(release.restored_from_id)}` : release.reason === 'initial' ? '初始版本' : release.reason}</TableCell><TableCell className="subtle">{moment(release.created_at)}</TableCell><TableCell className="text-right">{release.active ? <span className="positive">当前</span> : canAdmin ? <button className="text-link" onClick={() => restore(release)}>恢复</button> : <span className="subtle">—</span>}</TableCell></TableRow>)}</TableBody></Table></div>{!releases.length && <div className="empty">暂无发布记录</div>}</ShellSection></div></>}
      </main>
    </div>

    {searchOpen && <div className="dialog-backdrop command-backdrop" onMouseDown={() => setSearchOpen(false)}>
      <div className="command-panel" role="dialog" aria-modal="true" aria-label="全局搜索" onMouseDown={event => event.stopPropagation()}>
        <div className="command-search"><Search size={19} /><input autoFocus value={searchQuery} onChange={event => setSearchQuery(event.target.value)} placeholder="搜索数据集、特征或任务" aria-label="搜索内容" /><button onClick={() => setSearchOpen(false)} aria-label="关闭搜索"><X size={17} /></button></div>
        <div className="command-results">{globalHits.map(item => <button key={`${item.page}-${item.id}`} className="command-result" onClick={() => navigate(item.page, item.query)}><span className="command-result-icon">{item.page === 'sources' ? <Database size={16} /> : item.page === 'features' ? <GitBranch size={16} /> : <Workflow size={16} />}</span><span><strong>{item.title}</strong><small>{item.detail}</small></span><ArrowRight size={15} className="command-result-arrow" /></button>)}{!globalHits.length && <div className="empty">没有匹配的数据集、特征或最近任务</div>}</div>
        <div className="command-foot">按 Ctrl/⌘ + K 打开搜索 · Esc 关闭</div>
      </div>
    </div>}
    {modal === 'source' && <Dialog title="注册数据源" onClose={() => setModal(null)} onSave={saveSource} saving={saving}><div className="field"><label>名称</label><input required value={sourceForm.name} onChange={event => setSourceForm({ ...sourceForm, name: event.target.value })} placeholder="例如：销售订单" /></div><div className="field"><label>类型</label><select value={sourceForm.kind} onChange={event => setSourceForm({ ...sourceForm, kind: event.target.value as SourceKind })}><option value="file">文件</option><option value="sqlite">SQLite</option><option value="postgres">PostgreSQL</option><option value="http">HTTP JSON</option><option value="document">文档</option></select></div>{!['file', 'document'].includes(sourceForm.kind) && <div className="field col-span-full"><label>{sourceForm.kind === 'sqlite' ? '数据库文件相对路径' : sourceForm.kind === 'postgres' ? '连接凭据环境变量名' : 'JSON API URL'}</label><input required value={sourceForm.location} onChange={event => setSourceForm({ ...sourceForm, location: event.target.value })} /></div>}{['postgres', 'http'].includes(sourceForm.kind) && <>
      <div className="field"><label>主键列（可选增量）</label><input value={sourceForm.primary_key} onChange={event => setSourceForm({ ...sourceForm, primary_key: event.target.value })} placeholder="id" /></div>
      <div className="field"><label>更新时间列</label><input value={sourceForm.updated_at} onChange={event => setSourceForm({ ...sourceForm, updated_at: event.target.value })} placeholder="updated_at" /></div>
      <div className="field col-span-full"><label>软删除标记列（可选）</label><input value={sourceForm.deleted_column} onChange={event => setSourceForm({ ...sourceForm, deleted_column: event.target.value })} placeholder="is_deleted" /></div>
      </>}{sourceForm.kind === 'http' && <><div className="field"><label>记录字段路径（可选）</label><input value={sourceForm.records_path} onChange={event => setSourceForm({ ...sourceForm, records_path: event.target.value })} placeholder="payload.records" /></div><div className="field"><label>下一页字段路径（可选）</label><input value={sourceForm.next_path} onChange={event => setSourceForm({ ...sourceForm, next_path: event.target.value })} placeholder="paging.next" /></div><div className="field col-span-full"><label>字段映射（可选）</label><input value={sourceForm.field_map} onChange={event => setSourceForm({ ...sourceForm, field_map: event.target.value })} placeholder="orderId:order_id, amountCents:amount_cents" /><span className="subtle">主键、更新时间列和软删除列填写映射后的目录字段名。</span></div></>}<p className="subtle col-span-full">{['postgres', 'http'].includes(sourceForm.kind) ? '主键和更新时间列需同时填写；HTTP 增量接口接收 updated_after 参数。未填写时使用完整快照。' : '文件与文档从配置的数据源目录读取；数据集创建时指定文件名或表名。'}</p></Dialog>}
    {keyOpen && <Dialog title="设置实例访问密钥" onClose={() => setKeyOpen(false)} saving={false} onSave={event => { event.preventDefault(); sessionStorage.setItem('fluxplane-api-key', apiKey); setKeyOpen(false); setRole(null); setActiveKey(apiKey) }}><div className="field col-span-full"><label>API 密钥</label><input type="password" autoComplete="off" value={apiKey} onChange={event => setApiKey(event.target.value)} placeholder="输入当前实例密钥" /></div><p className="subtle col-span-full">密钥只保留在当前浏览器会话，用于访问本地部署实例。</p></Dialog>}
    {modal === 'dataset' && <Dialog title="创建数据集" onClose={() => setModal(null)} onSave={saveDataset} saving={saving}><div className="field"><label>SQL 名称</label><input required value={datasetForm.name} onChange={event => setDatasetForm({ ...datasetForm, name: event.target.value })} placeholder="orders" /></div><div className="field"><label>所属数据源</label><select required value={datasetForm.source_id} onChange={event => setDatasetForm({ ...datasetForm, source_id: event.target.value })}>{sources.map(source => <option key={source.id} value={source.id}>{source.name}</option>)}</select></div><div className="field col-span-full"><label>来源对象（相对文件路径或表名）</label><input required value={datasetForm.source_object} onChange={event => setDatasetForm({ ...datasetForm, source_object: event.target.value })} placeholder="orders.csv" /></div></Dialog>}
    {modal === 'workflow' && <Dialog title="新建执行计划" onClose={() => setModal(null)} onSave={saveWorkflow} saving={saving}>
      <div className="field col-span-full"><label>计划名称</label><input required value={workflowForm.name} onChange={event => setWorkflowForm({ ...workflowForm, name: event.target.value })} placeholder="例如：每日经营报表" /></div>
      <div className="field col-span-full"><label>同步数据集</label><div className="grid grid-cols-1 sm:grid-cols-2 gap-2">{datasets.map(item => <label key={item.id} className="flex items-center gap-2 text-sm"><input type="checkbox" checked={workflowForm.datasetIds.includes(item.id)} onChange={() => setWorkflowForm(current => ({ ...current, datasetIds: current.datasetIds.includes(item.id) ? current.datasetIds.filter(id => id !== item.id) : [...current.datasetIds, item.id] }))} />{item.name}</label>)}</div></div>
      <div className="field col-span-full"><label>刷新特征</label><div className="grid grid-cols-1 sm:grid-cols-2 gap-2">{features.map(item => <label key={item.id} className="flex items-center gap-2 text-sm"><input type="checkbox" checked={workflowForm.featureIds.includes(item.id)} onChange={() => setWorkflowForm(current => ({ ...current, featureIds: current.featureIds.includes(item.id) ? current.featureIds.filter(id => id !== item.id) : [...current.featureIds, item.id] }))} />{item.name}</label>)}</div></div>
      <div className="field col-span-full"><label>完成后查询（可选）</label><textarea value={workflowForm.sql} onChange={event => setWorkflowForm({ ...workflowForm, sql: event.target.value })} placeholder="SELECT * FROM your_feature" /></div>
      <p className="subtle col-span-full">刷新等待所依赖的数据同步完成；查询等待本计划的同步和刷新完成。</p>
    </Dialog>}
    {modal === 'layout' && <Dialog title="整理数据分区" onClose={() => setModal(null)} onSave={saveLayout} saving={saving}><div className="field col-span-full"><label>数据集</label><input readOnly value={datasets.find(item => item.id === layoutForm.dataset_id)?.name ?? ''} /></div><div className="field col-span-full"><label>常用等值过滤字段</label><input required value={layoutForm.partition_key} onChange={event => setLayoutForm({ ...layoutForm, partition_key: event.target.value })} placeholder="region" /></div><p className="subtle col-span-full">后台合并分区文件并验证结果；只有过滤读取更快且空间开销受控时才启用。</p></Dialog>}
    {modal === 'feature' && <Dialog title="新建特征" onClose={() => setModal(null)} onSave={saveFeature} saving={saving}>
      <div className="field"><label>名称</label><input required value={featureForm.name} onChange={event => setFeatureForm({ ...featureForm, name: event.target.value })} placeholder="customer_spend" /></div>
      <div className="field"><label>实体键</label><input required value={featureForm.entity_keys} onChange={event => setFeatureForm({ ...featureForm, entity_keys: event.target.value })} placeholder="customer_id" /></div>
      <div className="field col-span-full"><label>依赖数据集{['sql', 'window_sql'].includes(featureForm.mode) ? '（逗号分隔）' : '（单一来源）'}</label><input required value={featureForm.dependencies} onChange={event => setFeatureForm({ ...featureForm, dependencies: event.target.value })} placeholder="orders" /></div>
      <div className="field col-span-full"><label>计算方式</label><select value={featureForm.mode} onChange={event => setFeatureForm({ ...featureForm, mode: event.target.value })}><option value="sql">SQL 全量计算</option><option value="window_sql">跨源事件窗口 SQL</option><option value="append_sum_count">追加数据增量聚合</option><option value="rolling_sum">事件时间滚动聚合</option><option value="python">Python · 文本匹配计数</option></select></div>
      {['sql', 'window_sql'].includes(featureForm.mode) ? <><div className="field col-span-full"><label>特征 SQL</label><textarea required value={featureForm.sql} onChange={event => setFeatureForm({ ...featureForm, sql: event.target.value })} placeholder="SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id" /></div>{featureForm.mode === 'sql' ? <div className="field col-span-full"><label>定时刷新间隔（秒，可选）</label><input type="number" min="1" value={featureForm.refresh_interval_seconds} onChange={event => setFeatureForm({ ...featureForm, refresh_interval_seconds: event.target.value })} placeholder="滚动窗口等随时间变化的计算可设置" /></div> : <div className="field col-span-full"><label>输入窗口</label><div className="space-y-2">{featureForm.dependencies.split(',').map(name => name.trim()).filter(Boolean).map(name => { const rule = windowRules[name] ?? { enabled: false, event_time: '', window_seconds: '' }; return <div key={name} className="flex flex-wrap items-center gap-2"><label className="flex items-center gap-2 min-w-28"><input type="checkbox" checked={rule.enabled} onChange={event => setWindowRules(current => ({ ...current, [name]: { ...rule, enabled: event.target.checked } }))} />{name}</label><input aria-label={`${name} 事件时间字段`} disabled={!rule.enabled} required={rule.enabled} className="flex-1 min-w-36" value={rule.event_time} onChange={event => setWindowRules(current => ({ ...current, [name]: { ...rule, event_time: event.target.value } }))} placeholder="事件时间字段" /><input aria-label={`${name} 窗口秒数`} disabled={!rule.enabled} required={rule.enabled} type="number" min="1" className="w-36" value={rule.window_seconds} onChange={event => setWindowRules(current => ({ ...current, [name]: { ...rule, window_seconds: event.target.value } }))} placeholder="窗口秒数" /></div> })}</div><p className="subtle mt-2">勾选需要按事件时间过滤的输入；未勾选的数据集保持完整快照。</p></div>}</> : featureForm.mode === 'python' ? <>
        <div className="field"><label>文本字段</label><input required value={featureForm.text_column} onChange={event => setFeatureForm({ ...featureForm, text_column: event.target.value })} /></div>
        <div className="field"><label>结果字段</label><input required value={featureForm.output_column} onChange={event => setFeatureForm({ ...featureForm, output_column: event.target.value })} /></div>
        <div className="field col-span-full"><label>匹配词（逗号分隔）</label><input required value={featureForm.terms} onChange={event => setFeatureForm({ ...featureForm, terms: event.target.value })} placeholder="refund, return" /></div>
        <p className="subtle col-span-full">内置 Python 算子 v1，按行统计匹配次数；输入更新后自动重新计算。</p>
      </> : <>
        <div className="field"><label>求和字段</label><input required value={featureForm.measure} onChange={event => setFeatureForm({ ...featureForm, measure: event.target.value })} placeholder="amount" /></div>
        <div className="field"><label>求和结果名</label><input required value={featureForm.sum_alias} onChange={event => setFeatureForm({ ...featureForm, sum_alias: event.target.value })} /></div>
        <div className="field"><label>计数结果名</label><input required value={featureForm.count_alias} onChange={event => setFeatureForm({ ...featureForm, count_alias: event.target.value })} /></div>
        {featureForm.mode === 'rolling_sum' && <><div className="field"><label>事件时间字段</label><input required value={featureForm.event_time} onChange={event => setFeatureForm({ ...featureForm, event_time: event.target.value })} placeholder="occurred_at" /></div><div className="field"><label>窗口长度（秒）</label><input required type="number" min="1" value={featureForm.window_seconds} onChange={event => setFeatureForm({ ...featureForm, window_seconds: event.target.value })} placeholder="2592000" /></div></>}
        <p className="subtle col-span-full">{featureForm.mode === 'rolling_sum' ? '按事件进入和离开窗口的时刻刷新；仅支持单一数据集。' : '仅在来源确认为追加时使用增量计算；其他变化自动完整重算。'}</p>
      </>}
    </Dialog>}
    {selectedJob && <div className="dialog-backdrop" onMouseDown={() => setSelectedJob(null)}><div className="dialog" onMouseDown={event => event.stopPropagation()}><div className="dialog-head">任务详情 · {short(selectedJob.id)}<button onClick={() => setSelectedJob(null)} aria-label="关闭"><X size={18} /></button></div><div className="dialog-body"><div className="mb-4 flex gap-4 text-sm"><span className={statusTone(selectedJob.status)}>● {statusText(selectedJob.status)}</span><span className="subtle"><Clock3 size={13} className="inline" /> {moment(selectedJob.created_at)}</span></div><div className="subtle mb-2">任务 ID：{selectedJob.id}</div><div className="subtle mb-4">执行池：{selectedJob.lane} · 尝试次数：{selectedJob.attempt}</div>{selectedJob.cpu_millicores && <div className="subtle mb-2">资源预留：{selectedJob.cpu_millicores} mCPU · {selectedJob.memory_mb} MiB</div>}{selectedJob.runtime && <div className="subtle mb-4">实测：{selectedJob.runtime.duration_ms.toFixed(1)} ms 耗时 · {selectedJob.runtime.cpu_ms?.toFixed(1) ?? "—"} ms CPU · {selectedJob.runtime.peak_rss_mb?.toFixed(1) ?? "—"} MiB 进程峰值 RSS</div>}{selectedJob.error && <div className="notice">{selectedJob.error}</div>}{selectedJob.result && <pre className="code rounded-lg bg-[var(--muted)] p-3 overflow-auto max-h-80">{JSON.stringify(selectedJob.result, null, 2)}</pre>}<div className="flex gap-2 mt-5">{canOperate && ['pending', 'running'].includes(selectedJob.status) && <Button variant="outline" onClick={() => void run(() => post(`/runs/${selectedJob.id}/cancel`), '取消请求已提交').then(() => setSelectedJob(null))}>取消任务</Button>}{canOperate && selectedJob.status === 'failed' && <Button onClick={() => void run(() => post(`/runs/${selectedJob.id}/retry`), '任务已重新排队').then(() => setSelectedJob(null))}>重试任务</Button>}</div></div></div></div>}
    {toast && <div role="status" className="toast"><CheckCircle2 size={14} className="mr-2 inline" />{toast}</div>}
  </div>
}
