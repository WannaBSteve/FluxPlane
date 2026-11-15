import { writeFileSync } from 'node:fs';

const X = [28, 348, 668, 988];
const node = (id, layer, col, y, title, sub, lines, footer, extra = {}) => ({
  id, layer, x: X[col], y, w: 244, h: 170, title, sub, lines, footer, ...extra,
});
const across = (from, to, flow, label, col, y, extra = {}) => ({
  from: `${from}.r`, to: `${to}.l`, flow, label,
  labelPos: [X[col] + 282, y + 64], ...extra,
});

const spec = {
  lang: 'zh-CN', theme: 'saas', width: 1280,
  pageTitle: 'FluxPlane · 多模态特征融合',
  title: 'FluxPlane · 多模态特征融合',
  subtitle: '连接表与文档，持续更新特征，用自然语言分析。',
  layers: [
    { id: 'interface', label: '交互与分析', color: 'blue' },
    { id: 'compute', label: '计算与维护', color: 'green' },
    { id: 'state', label: '目录与存储', color: 'cyan' },
    { id: 'external', label: '外部数据', color: 'amber' },
  ],
  flows: [
    { id: 'request', label: '请求与计划', color: 'blue' },
    { id: 'data', label: '数据与结果', color: 'cyan' },
    { id: 'task', label: '调度与更新', color: 'green', dash: true },
    { id: 'metadata', label: '定义与状态', color: 'amber', dash: true },
  ],
  groups: [
    { label: '01  数据接入与特征融合', x: 12, y: 202, w: 1256, h: 225, color: 'green' },
    { label: '02  自然语言分析', x: 12, y: 492, w: 1256, h: 225, color: 'blue' },
    { label: '03  调度与持续维护', x: 12, y: 792, w: 1256, h: 225, color: 'cyan' },
  ],
  nodes: [
    node('sources', 'external', 0, 230, '多源数据', '结构化记录 + 文档文本',
      ['数据库 · 文件 · HTTP JSON', 'TXT / Markdown / 文本 PDF'], '订单 · 商品 · 退款 · 业务说明'),
    node('ingest', 'compute', 1, 230, '数据接入', '连接器 · 同步任务',
      ['识别来源变化，形成数据快照', '提取字段并统计数据分布'], '数据库、文件、接口与文档'),
    node('features', 'compute', 2, 230, '特征融合', 'SQL · 时间窗口 · 文本算子',
      ['按实体关联多源信息', '依赖数据变化后刷新特征'], '关联 · 聚合 · 文本特征'),
    node('published', 'state', 3, 230, '已发布数据', '版本目录 + Parquet',
      ['数据快照与融合特征', '记录版本组合与产物位置'], '版本发布 · 历史恢复'),
    node('workbench', 'interface', 0, 520, '分析工作台', '自然语言 / SQL',
      ['选择表、文档与特征', '提问，检查计划并确认执行'], '数据目录 · REST API · 权限'),
    node('planner', 'interface', 1, 520, '分析规划', '问题 → 可执行查询',
      ['根据字段和特征定义生成 SQL', '检查字段与查询范围'], '分析模型 · 只读检查'),
    node('query', 'compute', 2, 520, '查询执行', '查询 worker · DuckDB',
      ['读取固定版本的数据与特征', '执行计算或复用已有结果'], 'Arrow · 查询物化'),
    node('result', 'interface', 3, 520, '分析结果', '表格 / 图表 / 历史查询',
      ['查看结果与所用版本', '跟踪任务进度和实际耗时'], '分页浏览 · API · SSE'),
    node('scheduler', 'compute', 0, 820, '任务调度', '依赖 · 优先级 · 资源配额',
      ['查询与后台两个执行池', '任务租约、重试与空闲借用'], '同步 · 刷新 · 查询'),
    node('catalog', 'state', 1, 820, '共享目录与任务', 'PostgreSQL / SQLite',
      ['数据源与字段  /  特征定义与依赖', '任务记录与状态  /  版本组合与发布历史'],
      'API、调度器与 worker 共享持久记录', { w: 564 }),
    node('optimize', 'compute', 3, 820, '后台优化', '查询复用 · 数据分区',
      ['为稳定的重复查询构建物化结果', '按常用过滤字段组织分区'], '验证结果 · 记录收益'),
  ],
  edges: [
    across('sources', 'ingest', 'data', '接入', 0, 230),
    across('ingest', 'features', 'task', '更新', 1, 230),
    across('features', 'published', 'data', '发布', 2, 230),
    { from: 'ingest.t', to: 'published.t', flow: 'data', mid: 173,
      label: '数据快照', labelPos: [790, 159] },
    across('workbench', 'planner', 'request', '问题 / 计划', 0, 520, { arrow: 'both' }),
    across('planner', 'query', 'request', '确认执行', 1, 520),
    across('query', 'result', 'data', '结果', 2, 520),
    { from: 'published.b@0.35', to: 'query.t@0.65', flow: 'data', mid: 458,
      label: '固定版本', labelPos: [950, 445] },
    { from: 'catalog.t@122px', to: 'planner.b', flow: 'metadata',
      label: '字段定义', labelPos: [512, 750] },
    across('scheduler', 'catalog', 'metadata', '任务状态', 0, 820),
    { from: 'catalog.t@462px', to: 'query.b@142px', flow: 'task', arrow: 'both',
      label: '任务 / 状态', labelPos: [760, 750] },
    { from: 'query.b@0.92', to: 'optimize.t@0.25', flow: 'task', mid: 766,
      label: '重复查询', labelPos: [958, 750] },
    { from: 'optimize.r', to: 'published.r@0.72', flow: 'data', rail: 'x:1256',
      label: '复用产物', labelPos: [1217, 750] },
  ],
};

writeFileSync(new URL('./fluxplane.spec.json', import.meta.url), JSON.stringify(spec, null, 2) + '\n');
