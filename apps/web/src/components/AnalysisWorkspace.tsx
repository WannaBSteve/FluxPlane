import { useEffect, useState } from "react";
import {
  ArrowRight,
  CheckCircle2,
  Circle,
  Clock3,
  Code2,
  Database,
  GitBranch,
  MessageSquare,
  Play,
  Search,
  Table2,
} from "lucide-react";
import {
  api,
  post,
  short,
  moment,
  type Dataset,
  type Feature,
  type Job,
  type QueryResultPage,
} from "@/lib/api";
import type { AnalysisPlan, AnalysisStatus } from "@/lib/generated/types.gen";
import { Button } from "@/components/ui/button";
import { DataGrid } from "./DataExplorer";

function numericColumns(value: QueryResultPage) {
  return value.columns.filter(column => {
    const values = value.rows.map(row => row[column]).filter(item => item != null);
    return values.length > 0 && values.every(item =>
      (typeof item === "number" && Number.isFinite(item)) ||
      (typeof item === "string" && /^-?\d+(\.\d+)?([eE][+-]?\d+)?$/.test(item) && Number.isFinite(Number(item))));
  });
}

const demoQueries = [
  {
    label: "各品类销售额",
    relations: ["demo_orders", "demo_products"],
    sql: `SELECT p.category, COUNT(*) AS order_count,
  SUM(o.amount_cents) AS gross_cents
FROM demo_orders AS o
JOIN demo_products AS p ON o.product_id = p.product_id
GROUP BY p.category
ORDER BY p.category`,
  },
  {
    label: "退款后的净额",
    relations: ["demo_orders", "demo_products", "demo_refunds"],
    sql: `WITH refunds AS (
  SELECT order_id, SUM(amount_cents) AS refunded_cents
  FROM demo_refunds GROUP BY order_id
)
SELECT p.category,
  SUM(o.amount_cents - COALESCE(r.refunded_cents, 0)) AS net_cents
FROM demo_orders AS o
JOIN demo_products AS p ON o.product_id = p.product_id
LEFT JOIN refunds AS r ON o.order_id = r.order_id
GROUP BY p.category
ORDER BY p.category`,
  },
  {
    label: "退货相关文档",
    relations: ["demo_policy_topics"],
    sql: `SELECT source, chunk_index, topic
FROM demo_policy_topics
WHERE topic = 'returns'
ORDER BY source, chunk_index
LIMIT 20`,
  },
];

export default function AnalysisWorkspace({
  datasets,
  features,
  jobs,
  initialRelation,
  canOperate,
  onRefresh,
}: {
  datasets: Dataset[];
  features: Feature[];
  jobs: Job[];
  initialRelation: string;
  canOperate: boolean;
  onRefresh: () => Promise<void>;
}) {
  const [cpu, setCpu] = useState(500);
  const [memory, setMemory] = useState(512);
  const [mode, setMode] = useState<"natural" | "sql">("natural");
  const [question, setQuestion] = useState("");
  const [sql, setSql] = useState("");
  const [selected, setSelected] = useState<string[]>(
    initialRelation ? [initialRelation] : [],
  );
  const [search, setSearch] = useState("");
  const [status, setStatus] = useState<AnalysisStatus | null>(null);
  const [plan, setPlan] = useState<AnalysisPlan | null>(null);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");
  const [jobId, setJobId] = useState<string | null>(() =>
    sessionStorage.getItem("fluxplane-analysis-job"),
  );
  const [job, setJob] = useState<Job | null>(null);
  const [page, setPage] = useState(0);
  const [result, setResult] = useState<QueryResultPage | null>(null);
  const [chart, setChart] = useState(false);
  const [metric, setMetric] = useState("");
  const [category, setCategory] = useState("");
  const relations = [
    ...datasets.map((d) => ({
      name: d.name,
      ready: !!d.active_version_id,
      feature: false,
    })),
    ...features.map((f) => ({
      name: f.name,
      ready: !!f.active_snapshot_id && !f.stale,
      feature: true,
    })),
  ];
  useEffect(() => {
    api<AnalysisStatus>("/analysis/status")
      .then(value => {
        setStatus(value);
        if (!value.configured) setMode("sql");
      })
      .catch((e) => setError(e.message));
  }, []);
  useEffect(() => {
    if (initialRelation) {
      setSelected([initialRelation]);
      setPlan(null);
    }
  }, [initialRelation]);
  useEffect(() => {
    if (!jobId) return;
    sessionStorage.setItem("fluxplane-analysis-job", jobId);
    let stopped = false;
    let timer: ReturnType<typeof setTimeout>;
    const poll = async () => {
      try {
        const value = await api<Job>(`/runs/${jobId}`);
        if (stopped) return;
        setJob(value);
        if (["pending", "running", "cancelling"].includes(value.status))
          timer = setTimeout(poll, 1500);
      } catch (e) {
        if (!stopped) setError(String(e));
      }
    };
    void poll();
    return () => {
      stopped = true;
      clearTimeout(timer);
    };
  }, [jobId]);
  useEffect(() => {
    setPage(0);
    setResult(null);
  }, [jobId]);
  useEffect(() => {
    if (!job || job.id !== jobId || job.status !== "succeeded") return;
    const controller = new AbortController();
    setResult(null);
    api<QueryResultPage>(`/runs/${job.id}/rows?offset=${page * 25}&limit=25`, {
      signal: controller.signal,
    })
      .then((value) => {
        setResult(value);
        const numbers = numericColumns(value);
        const number = numbers.find(c => !/(^id$|_id$)/i.test(c)) ?? numbers[0] ?? "";
        const label =
          value.columns.find((c) =>
            !numbers.includes(c),
          ) ??
          value.columns[0] ??
          "";
        setMetric(number);
        setCategory(label);
      })
      .catch((e) => {
        if (!controller.signal.aborted) setError(e.message);
      });
    return () => controller.abort();
  }, [job, jobId, page]);
  const generate = async () => {
    setBusy("planning");
    setError("");
    setPlan(null);
    setJob(null);
    setJobId(null);
    setResult(null);
    sessionStorage.removeItem("fluxplane-analysis-job");
    try {
      setPlan(
        await post<AnalysisPlan>("/analysis/plan", {
          question,
          relations: selected,
        }),
      );
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy("");
    }
  };
  const execute = async () => {
    setBusy("submitting");
    setError("");
    try {
      const value = await post<Job>(
        "/queries",
        mode === "natural"
          ? {
              sql: plan?.sql,
              question,
              expected_versions: plan?.expected_versions,
              cpu_millicores: cpu,
              memory_mb: memory,
            }
          : { sql, cpu_millicores: cpu, memory_mb: memory },
      );
      setJob(value);
      setJobId(value.id);
      setPage(0);
      setResult(null);
      await onRefresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy("");
    }
  };
  const change = () => {
    setPlan(null);
  };
  const running =
    job && ["pending", "running", "cancelling"].includes(job.status);
  const locked = !!busy || !!running;
  const recent = jobs.filter((j) => j.kind === "query").slice(0, 8);
  const numeric = result ? numericColumns(result) : [];
  const max = Math.max(
    1,
    ...(result?.rows.map((r) => Math.abs(Number(r[metric]) || 0)) ?? []),
  );
  return (
    <div className="analysis-layout">
      <aside className="surface analysis-data">
        <div className="surface-head">
          <div>
            <div className="surface-title">分析范围</div>
            <div className="surface-sub">选择这次要使用的数据</div>
          </div>
          <Database size={18} />
        </div>
        <div className="analysis-data-search">
          <Search size={14} />
          <input
            aria-label="搜索分析数据"
            placeholder="搜索表或特征"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </div>
        {[
          { title: "数据集与文档", feature: false },
          { title: "已计算特征", feature: true },
        ].map((group) => (
          <div className="relation-group" key={group.title}>
            <h3>{group.title}</h3>
            {relations
              .filter(
                (r) =>
                  r.feature === group.feature &&
                  r.name.toLowerCase().includes(search.toLowerCase()),
              )
              .map((r) => (
                <label
                  key={r.name}
                  title={r.ready ? r.name : "需要先同步或刷新"}
                >
                  <input
                    type="checkbox"
                    disabled={!r.ready || locked}
                    checked={selected.includes(r.name)}
                    onChange={(e) => {
                      setSelected((s) =>
                        e.target.checked
                          ? [...s, r.name]
                          : s.filter((n) => n !== r.name),
                      );
                      change();
                    }}
                  />
                  {r.feature ? <GitBranch size={15} /> : <Table2 size={15} />}
                  <span>{r.name}</span>
                  {!r.ready && <small>待更新</small>}
                </label>
              ))}
          </div>
        ))}
        <div className="analysis-selection">
          已选 {selected.length} 项{" "}
          <button
            className="text-link"
            disabled={locked}
            onClick={() => {
              setSelected([]);
              change();
            }}
          >
            清空
          </button>
        </div>
      </aside>
      <div className="analysis-main">
        <section className="surface question-panel">
          <div className="analysis-mode">
            <div>
              <MessageSquare size={18} />
              <strong>从一个问题开始</strong>
            </div>
            <div className="mode-switch">
              <button
                aria-pressed={mode === "natural"}
                disabled={locked}
                onClick={() => setMode("natural")}
              >
                自然语言
              </button>
              <button
                aria-pressed={mode === "sql"}
                disabled={locked}
                onClick={() => setMode("sql")}
              >
                SQL
              </button>
            </div>
          </div>
          {mode === "natural" ? (
            <>
              <textarea
                aria-label="分析问题"
                disabled={locked}
                value={question}
                onChange={(e) => {
                  setQuestion(e.target.value);
                  change();
                }}
                placeholder="例如：按商品类别汇总销售额，找出贡献最高的类别。"
              />
              <div className="question-examples">
                <span>试着问</span>
                {[
                  "列出最近的 20 条记录",
                  "按商品类别统计销售额和订单数",
                  "查找服务文档中提到退货期限的段落",
                ].map((q) => (
                  <button
                    disabled={locked}
                    key={q}
                    onClick={() => {
                      setQuestion(q);
                      change();
                    }}
                  >
                    {q}
                  </button>
                ))}
              </div>
              <div className="analysis-submit">
                <span>
                  {status?.configured
                    ? `分析模型 · ${status.model}`
                    : "尚未连接分析模型 · SQL 查询可直接使用"}
                </span>
                <Button
                  disabled={
                    !canOperate ||
                    locked ||
                    question.trim().length < 2 ||
                    !selected.length ||
                    !status?.configured
                  }
                  onClick={() => void generate()}
                >
                  {busy === "planning" ? "正在生成计划…" : "生成分析计划"}
                  <ArrowRight size={15} />
                </Button>
              </div>
              <details className="analysis-disclosure">
                <summary>模型使用哪些信息？</summary>
                <p>
                  {status?.context_policy ??
                    "仅发送问题、所选表的字段和特征定义，不发送数据行。"}
                  生成的计划会检查表名、字段和只读操作，确认后才执行。
                </p>
                {!status?.configured && (
                  <p>
                    在服务端配置
                    FLUXPLANE_ANALYSIS_BASE_URL、FLUXPLANE_ANALYSIS_MODEL 和
                    FLUXPLANE_ANALYSIS_API_KEY，然后重启 API。支持 OpenAI
                    兼容接口。
                  </p>
                )}
              </details>
            </>
          ) : (
            <>
              {demoQueries.some(example => example.relations.every(name =>
                relations.some(relation => relation.name === name && relation.ready))) && (
                <div className="question-examples">
                  <span>演示查询</span>
                  {demoQueries.filter(example => example.relations.every(name =>
                    relations.some(relation => relation.name === name && relation.ready)))
                    .map(example => (
                      <button
                        key={example.label}
                        disabled={locked}
                        onClick={() => {
                          setSelected(example.relations);
                          setSql(example.sql);
                          setPlan(null);
                        }}
                      >
                        {example.label}
                      </button>
                    ))}
                </div>
              )}
              <textarea
                className="code"
                aria-label="SQL 查询"
                value={sql}
                disabled={locked}
                onChange={(e) => setSql(e.target.value)}
                placeholder={
                  selected[0]
                    ? `SELECT * FROM "${selected[0]}" LIMIT 20`
                    : "SELECT ..."
                }
              />
              <div className="analysis-submit">
                <button
                  className="text-link"
                  disabled={!selected.length || locked}
                  onClick={() =>
                    setSql(`SELECT * FROM "${selected[0]}" LIMIT 20`)
                  }
                >
                  预览选中的表
                </button>
                <Button
                  disabled={!canOperate || locked || !sql.trim()}
                  onClick={() => void execute()}
                >
                  <Play size={14} />
                  执行查询
                </Button>
              </div>
              <p className="surface-sub sql-scope-note">
                {status?.configured
                  ? "SQL 模式可查询目录中所有已发布数据，左侧选择用于快速插入表名。"
                  : "演示查询无需模型服务。连接模型后，也可以切换到自然语言生成操作计划。"}
              </p>
            </>
          )}
          <details className="analysis-disclosure">
            <summary>执行资源</summary>
            <div className="chart-controls">
              <label>
                CPU（millicores）{" "}
                <input
                  type="number"
                  min="1"
                  value={cpu}
                  onChange={(e) => setCpu(Number(e.target.value))}
                />
              </label>
              <label>
                内存（MiB）{" "}
                <input
                  type="number"
                  min="1"
                  value={memory}
                  onChange={(e) => setMemory(Number(e.target.value))}
                />
              </label>
            </div>
          </details>
        </section>
        {error && (
          <div className="notice" role="alert">
            {error}
          </div>
        )}
        {(busy === "planning" || plan) && mode === "natural" && (
          <section className="surface operation-plan">
            <div className="surface-head">
              <div>
                <div className="surface-title">
                  {plan?.clarification ? "还需要确认" : "操作计划"}
                </div>
                <div className="surface-sub">
                  {busy === "planning"
                    ? "根据所选字段组织查询，并检查可执行性"
                    : plan?.summary}
                </div>
              </div>
              <GitBranch size={19} />
            </div>
            {plan?.clarification ? (
              <div className="surface-body">
                <p>{plan.clarification}</p>
                <p className="subtle">补充上方问题后，重新生成计划。</p>
              </div>
            ) : (
              plan && (
                <>
                  <div className="operation-flow">
                    {plan.operations.map((op, i) => (
                      <div className="operation-node" key={`${i}-${op}`}>
                        <span>{String(i + 1).padStart(2, "0")}</span>
                        <strong>{op}</strong>
                        {i < plan.operations.length - 1 && (
                          <ArrowRight size={15} />
                        )}
                      </div>
                    ))}
                  </div>
                  <div className="plan-inputs">
                    {plan.relations.map((name) => (
                      <span key={name}>
                        <Database size={13} />
                        {name}
                        <code>{short(plan.expected_versions[name])}</code>
                      </span>
                    ))}
                  </div>
                  {!!plan.assumptions?.length && (
                    <div className="plan-assumptions">
                      <strong>统计口径</strong>
                      <ul>
                        {plan.assumptions?.map((a) => (
                          <li key={a}>{a}</li>
                        ))}
                      </ul>
                    </div>
                  )}
                  <div className="plan-confirm">
                    <details>
                      <summary>
                        <Code2 size={14} />
                        查看 SQL
                      </summary>
                      <pre>{plan.sql}</pre>
                    </details>
                    <Button
                      disabled={!canOperate || locked || !plan.sql}
                      onClick={() => void execute()}
                    >
                      <Play size={14} />
                      确认并执行
                    </Button>
                  </div>
                </>
              )
            )}
          </section>
        )}
        {job && (
          <section className="surface result-panel">
            <div className="surface-head">
              <div>
                <div className="surface-title">
                  {job.status === "succeeded"
                    ? "分析结果"
                    : job.status === "failed"
                      ? "执行失败"
                      : "查询进度"}
                </div>
                <div className="surface-sub">
                  任务 {short(job.id)} · {moment(job.created_at)}
                  {job.runtime
                    ? ` · ${job.runtime.duration_ms.toFixed(0)} ms`
                    : ""}
                </div>
                {job.query_question && (
                  <p className="query-question">{job.query_question}</p>
                )}
              </div>
              {result && (
                <div className="mode-switch">
                  <button aria-pressed={!chart} onClick={() => setChart(false)}>
                    表格
                  </button>
                  <button
                    aria-pressed={chart}
                    disabled={!numeric.length}
                    onClick={() => setChart(true)}
                  >
                    图表
                  </button>
                </div>
              )}
            </div>
            <div className="execution-track">
              {[
                { text: "已提交", done: true },
                {
                  text: job.status === "pending" ? "等待执行" : job.status === "running" ? "正在执行" : "执行查询",
                  done: job.status === "succeeded",
                },
                { text: "结果可用", done: job.status === "succeeded" },
              ].map((s) => (
                <span className={s.done ? "complete" : ""} key={s.text}>
                  {s.done ? <CheckCircle2 size={15} /> : <Circle size={15} />}
                  {s.text}
                </span>
              ))}
              {job.status === "failed" || job.status === "cancelled" ? (
                <strong className="negative">
                  {job.error ?? "任务已取消"}
                </strong>
              ) : null}
            </div>
            {job.status === "succeeded" && <div className="execution-facts">
              <span>{job.result?.cache_hit === true ? "复用已有查询结果" : "本次计算"}</span>
              {job.runtime?.cpu_ms != null && <span>CPU {job.runtime.cpu_ms.toFixed(0)} ms</span>}
              {job.runtime?.peak_rss_mb != null && <span>峰值内存 {job.runtime.peak_rss_mb.toFixed(1)} MiB</span>}
            </div>}
            {!!job.result?.versions && (
              <details className="result-versions">
                <summary>查看执行时的目录版本</summary>
                <pre>{JSON.stringify(job.result.versions, null, 2)}</pre>
              </details>
            )}
            {result && (
              <>
                {chart ? (
                  <div className="result-bars">
                    <div className="chart-controls">
                      <label>
                        分类{" "}
                        <select
                          value={category}
                          onChange={(e) => setCategory(e.target.value)}
                        >
                          {result.columns.map((c) => (
                            <option key={c}>{c}</option>
                          ))}
                        </select>
                      </label>
                      <label>
                        数值{" "}
                        <select
                          value={metric}
                          onChange={(e) => setMetric(e.target.value)}
                        >
                          {numeric.map((c) => (
                            <option key={c}>{c}</option>
                          ))}
                        </select>
                      </label>
                      <span>仅展示当前页，不另做聚合</span>
                    </div>
                    {result.rows.map((r, i) => (
                      <div className="result-bar" key={i}>
                        <span title={String(r[category])}>
                          {String(r[category] ?? i + 1)}
                        </span>
                        <div>
                          <i
                            style={{
                              width: `${(Math.abs(Number(r[metric]) || 0) / max) * 100}%`,
                              background:
                                Number(r[metric]) < 0 ? "#bb765d" : undefined,
                            }}
                          />
                        </div>
                        <strong>{String(r[metric] ?? "—")}</strong>
                      </div>
                    ))}
                  </div>
                ) : (
                  <DataGrid columns={result.columns} rows={result.rows} />
                )}
                <div className="task-pagination">
                  <span>
                    共 {result.row_count.toLocaleString()} 行 · 第 {page + 1} 页
                  </span>
                  <div>
                    <Button
                      variant="outline"
                      disabled={!page}
                      onClick={() => setPage((p) => p - 1)}
                    >
                      上一页
                    </Button>
                    <Button
                      variant="outline"
                      disabled={!result.has_more}
                      onClick={() => setPage((p) => p + 1)}
                    >
                      下一页
                    </Button>
                  </div>
                </div>
              </>
            )}
          </section>
        )}
        {!job && !plan && !busy && (
          <div className="analysis-empty">
            <div>
              <Database />
              <ArrowRight />
              <GitBranch />
              <ArrowRight />
              <Table2 />
            </div>
            <h3>让表、文档和特征一起回答问题</h3>
            <p>
              选择数据，描述你想知道什么。执行前可以检查每一步操作和统计口径。
            </p>
          </div>
        )}
        {!!recent.length && (
          <section className="surface query-history">
            <div className="surface-head">
              <div className="surface-title">最近查询</div>
              <Clock3 size={17} />
            </div>
            {recent.map((j) => (
              <button
                disabled={locked}
                key={j.id}
                onClick={() => {
                  setJob(j);
                  setJobId(j.id);
                  setPlan(null);
                  setError("");
                  if (j.query_sql) setSql(j.query_sql);
                  if (j.query_question) setQuestion(j.query_question);
                }}
              >
                <span
                  className="query-history-label"
                  title={j.query_question ?? j.query_sql ?? j.id}
                >
                  {j.query_question ?? j.query_sql ?? short(j.id)}
                </span>
                <span>{moment(j.created_at)}</span>
                <span
                  className={j.status === "succeeded" ? "positive" : "pending"}
                >
                  {{
                    succeeded: "已完成",
                    failed: "失败",
                    running: "运行中",
                    pending: "等待中",
                    cancelled: "已取消",
                  }[j.status] ?? j.status}
                </span>
                <ArrowRight size={14} />
              </button>
            ))}
          </section>
        )}
      </div>
    </div>
  );
}
