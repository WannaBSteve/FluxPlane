import { useEffect, useRef, useState } from "react";
import {
  ArrowLeft,
  ArrowRight,
  Database,
  FileText,
  GitBranch,
  Layers3,
  Search,
  Table2,
} from "lucide-react";
import {
  api,
  moment,
  short,
  type Dataset,
  type Feature,
  type Source,
  type DatasetProfile,
  type DatasetVersion,
  type FeatureSnapshot,
} from "@/lib/api";
import type { PreviewOut } from "@/lib/generated/types.gen";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

export function DataGrid({
  columns,
  rows,
}: {
  columns: string[];
  rows: Record<string, unknown>[];
}) {
  return (
    <div className="data-grid-scroll">
      <table className="data-grid">
        <thead>
          <tr>
            <th className="row-number">#</th>
            {columns.map((c) => (
              <th key={c}>{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i}>
              <td className="row-number">{i + 1}</td>
              {columns.map((c) => (
                <td key={c}>
                  <span
                    title={
                      typeof row[c] === "object"
                        ? JSON.stringify(row[c])
                        : String(row[c] ?? "")
                    }
                  >
                    {row[c] == null ? (
                      <em>NULL</em>
                    ) : typeof row[c] === "object" ? (
                      JSON.stringify(row[c])
                    ) : (
                      String(row[c])
                    )}
                  </span>
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {!rows.length && <div className="empty">当前没有数据行</div>}
    </div>
  );
}

const kindLabel: Record<string, string> = {
  file: "文件表",
  document: "文档",
  postgres: "PostgreSQL",
  sqlite: "SQLite",
  http: "HTTP 接口",
};
type Item = Dataset | Feature;
export function isFeatureItem(item: Item): item is Feature {
  return Array.isArray(item.dependencies);
}
function isDatasetItem(item: Item): item is Dataset {
  return typeof item.source_id === "string";
}

export default function DataExplorer({
  datasets,
  features,
  sources,
  mode = "datasets",
  filter = "",
  onAnalyze,
  onSync,
  onLayout,
}: {
  datasets: Dataset[];
  features: Feature[];
  sources: Source[];
  mode?: "datasets" | "features";
  filter?: string;
  onAnalyze: (name: string) => void;
  onSync?: (item: Item) => void;
  onLayout?: (item: Dataset) => void;
}) {
  const [group, setGroup] = useState("all");
  const pendingLink = useRef(filter);
  const [query, setQuery] = useState(filter);
  const [selected, setSelected] = useState<Item | null>(null);
  const [documentMode, setDocumentMode] = useState(true);
  const [tab, setTab] = useState("preview");
  const [page, setPage] = useState(0);
  const [preview, setPreview] = useState<PreviewOut | null>(null);
  const [profile, setProfile] = useState<DatasetProfile | null>(null);
  const [versions, setVersions] = useState<
    (DatasetVersion | FeatureSnapshot)[]
  >([]);
  const [version, setVersion] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const isFeature = selected ? isFeatureItem(selected) : mode === "features";
  const open = (item: Item) => {
    setSelected(item);
    setTab("preview");
    setPage(0);
    setVersion("");
    setPreview(null);
    setProfile(null);
    setVersions([]);
    setError("");
  };
  useEffect(() => {
    pendingLink.current = filter;
    setSelected(null);
    setQuery(filter);
    setGroup("all");
  }, [mode, filter]);
  useEffect(() => {
    if (!pendingLink.current) return;
    const item = [...datasets, ...features].find(value => value.name === pendingLink.current);
    if (item) {
      open(item);
      pendingLink.current = "";
    }
  }, [datasets, features, filter]);
  useEffect(() => {
    if (!selected) return;
    const controller = new AbortController();
    const kind = isFeatureItem(selected) ? "features" : "datasets";
    setLoading(true);
    setError("");
    setPreview(null);
    api<PreviewOut>(
      `/catalog/${kind}/${selected.id}/preview?offset=${page * 25}&limit=25${version ? `&version_id=${version}` : ""}`,
      { signal: controller.signal },
    )
      .then((value) => {
        setPreview(value);
        if (!version) setVersion(value.version_id);
      })
      .catch((e) => {
        if (!controller.signal.aborted) setError(e.message);
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [selected, page, version]);
  useEffect(() => {
    if (!selected) return;
    const controller = new AbortController();
    const feature = isFeatureItem(selected);
    api<(DatasetVersion | FeatureSnapshot)[]>(
      `/${feature ? "features" : "datasets"}/${selected.id}/${feature ? "snapshots" : "versions"}`,
      { signal: controller.signal },
    )
      .then(setVersions)
      .catch(() => {});
    if (!feature)
      api<DatasetProfile>(`/datasets/${selected.id}/profile`, {
        signal: controller.signal,
      })
        .then(setProfile)
        .catch(() => {});
    return () => controller.abort();
  }, [selected]);
  const all = mode === "datasets" ? datasets : features;
  const filtered = all.filter((item) =>
    `${item.name} ${isDatasetItem(item) ? item.source_object : item.dependencies.join(" ")}`
      .toLowerCase()
      .includes(query.toLowerCase()),
  );
  const visible = filtered.filter(
    (item) =>
      group === "all" ||
      (isDatasetItem(item)
        ? item.source_id === group
        : group === (item.maintenance_mode === "python" ? "text" : "table")),
  );
  const documentPreview =
    selected &&
    isDatasetItem(selected) &&
    sources.some((s) => s.id === selected.source_id && s.kind === "document") &&
    preview?.columns.includes("text");
  const related = selected
    ? features.filter((f) => f.dependencies.includes(selected.name))
    : [];
  const tabs = [
    { key: "preview", label: "内容预览" },
    { key: "fields", label: "字段与分布" },
    { key: "lineage", label: "数据关系" },
    { key: "versions", label: "版本历史" },
  ];

  if (selected)
    return (
      <section className="surface explorer-detail">
        <div className="detail-heading">
          <button
            className="text-link back-link"
            onClick={() => setSelected(null)}
          >
            <ArrowLeft size={15} />
            返回目录
          </button>
          <div className="detail-title">
            <span className="catalog-icon">
              {isFeature ? <GitBranch /> : documentPreview ? <FileText /> : <Table2 />}
            </span>
            <div>
              <small>{isFeature ? "融合特征" : documentPreview ? "文档" : "数据集"}</small>
              <h2>{selected.name}</h2>
              <p>
                {isDatasetItem(selected)
                  ? selected.source_object
                  : selected.dependencies.join(" + ")}
              </p>
            </div>
          </div>
          <div className="detail-actions">
            <Button onClick={() => onAnalyze(selected.name)}>
              用这份数据分析
              <ArrowRight size={14} />
            </Button>
            {onSync && (
              <Button variant="outline" onClick={() => onSync(selected)}>
                {isFeature ? "刷新特征" : "同步数据"}
              </Button>
            )}
            {onLayout && !isFeature && (
              <Button
                variant="outline"
                onClick={() => onLayout(selected as Dataset)}
              >
                分区设置
              </Button>
            )}
          </div>
        </div>
        <div className="detail-stats">
          <span>
            <strong>{preview?.row_count.toLocaleString() ?? "—"}</strong> 行
          </span>
          <span>
            <strong>{preview?.fields.length ?? "—"}</strong> 个字段
          </span>
          <span>
            预览版本 <code>{short(preview?.version_id)}</code>
          </span>
        </div>
        <div className="explorer-tabs" role="tablist" aria-label="数据详情">
          {tabs.map((t) => (
            <button
              role="tab"
              aria-selected={tab === t.key}
              key={t.key}
              onClick={() => setTab(t.key)}
            >
              {t.label}
            </button>
          ))}
        </div>
        {error && (
          <div className="notice" role="alert">
            {error}
          </div>
        )}
        {tab === "preview" && (
          <>
            {loading ? (
              <div className="empty">正在读取数据…</div>
            ) : (
              preview && (
                <>
                  {documentPreview && (
                    <div className="document-switch mode-switch">
                      <button
                        aria-pressed={documentMode}
                        onClick={() => setDocumentMode(true)}
                      >
                        阅读文档
                      </button>
                      <button
                        aria-pressed={!documentMode}
                        onClick={() => setDocumentMode(false)}
                      >
                        查看数据表
                      </button>
                    </div>
                  )}
                  {documentPreview && documentMode ? (
                    <div className="document-excerpts">
                      {preview.rows.map((row, index) => (
                        <article key={index}>
                          <div>
                            <FileText size={16} />
                            <strong>
                              {String(row.source ?? selected.name)}
                            </strong>
                            <span>
                              片段{" "}
                              {String(row.chunk_index ?? page * 25 + index)}
                            </span>
                          </div>
                          <p>{String(row.text ?? "")}</p>
                        </article>
                      ))}
                    </div>
                  ) : (
                    <DataGrid columns={preview.columns} rows={preview.rows} />
                  )}
                  <div className="task-pagination">
                    <span>
                      第 {page + 1} 页 · 共 {preview.row_count.toLocaleString()}{" "}
                      行 · 点击单元格可选中并复制内容
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
                        disabled={!preview.has_more}
                        onClick={() => setPage((p) => p + 1)}
                      >
                        下一页
                      </Button>
                    </div>
                  </div>
                </>
              )
            )}
          </>
        )}
        {tab === "fields" && (
          <div className="field-grid">
            {preview?.fields.map((field) => {
              const stat =
                profile?.version_id === preview.version_id
                  ? profile.fields.find((f) => f.name === field.name)
                  : null;
              return (
                <div className="field-card" key={field.name}>
                  <div>
                    <strong>{field.name}</strong>
                    <code>{field.type}</code>
                  </div>
                  {stat ? (
                    <>
                      <p>
                        空值 {stat.null_count} · 抽样不同值{" "}
                        {stat.distinct_in_sample ?? "—"}
                      </p>
                      {stat.distribution?.kind === "numeric" ? (
                        <div className="numeric-range">
                          <span>{stat.distribution.min}</span>
                          <i />
                          <span>{stat.distribution.max}</span>
                          <small>中位数 {stat.distribution.median}</small>
                        </div>
                      ) : (
                        stat.distribution?.top_values.map((v) => (
                          <div className="frequency" key={String(v.value)}>
                            <span>{String(v.value)}</span>
                            <meter
                              min="0"
                              max={Math.max(profile?.sampled_rows ?? 1, 1)}
                              value={v.count}
                            />
                            <small>{v.count}</small>
                          </div>
                        ))
                      )}
                    </>
                  ) : (
                    <p>字段类型来自当前预览版本</p>
                  )}
                </div>
              );
            })}
            {!preview && <div className="empty">数据同步后可查看字段</div>}
          </div>
        )}
        {tab === "lineage" && (
          <div className="lineage-detail">
            <div className="lineage-column">
              <small>输入数据</small>
              {isFeatureItem(selected) ? (
                selected.dependencies.map((name) => {
                  const item = [...datasets, ...features].find(
                    (d) => d.name === name,
                  );
                  return (
                    <button key={name} onClick={() => item && open(item)}>
                      <Database size={16} />
                      {name}
                      <ArrowRight size={14} />
                    </button>
                  );
                })
              ) : (
                <div className="lineage-node">
                  <Database size={16} />
                  {
                    sources.find(
                      (s) => s.id === (selected as Dataset).source_id,
                    )?.name
                  }
                </div>
              )}
            </div>
            <ArrowRight />
            <div className="lineage-focus">
              <Layers3 />
              {selected.name}
            </div>
            <ArrowRight />
            <div className="lineage-column">
              <small>下游特征</small>
              {related.map((f) => (
                <button key={f.id} onClick={() => open(f)}>
                  <GitBranch size={16} />
                  {f.name}
                  <ArrowRight size={14} />
                </button>
              ))}
              {!related.length && (
                <span className="subtle">尚无关联特征，可直接用于分析</span>
              )}
            </div>
            {isFeatureItem(selected) && selected.sql && (
              <details className="lineage-sql">
                <summary>查看特征定义</summary>
                <pre>{selected.sql}</pre>
              </details>
            )}
          </div>
        )}
        {tab === "versions" && (
          <div className="version-list">
            {versions.map((v) => (
              <button
                key={v.id}
                onClick={() => {
                  setVersion(v.id);
                  setPage(0);
                  setTab("preview");
                }}
              >
                <span className="version-dot" />
                <div>
                  <strong>
                    {typeof v.number === "number" ? `v${v.number}` : "特征快照"}
                  </strong>
                  <small>
                    {moment(v.created_at)} · {v.row_count.toLocaleString()} 行
                  </small>
                </div>
                <code>{short(v.id)}</code>
                <span>
                  查看内容 <ArrowRight size={13} />
                </span>
              </button>
            ))}
            {!versions.length && <div className="empty">尚无版本</div>}
          </div>
        )}
      </section>
    );

  return (
    <div className="catalog-layout">
      <aside className="surface catalog-groups">
        <div className="catalog-groups-title">
          {mode === "datasets" ? "按来源浏览" : "按特征类型浏览"}
        </div>
        <button
          className={group === "all" ? "selected" : ""}
          onClick={() => setGroup("all")}
        >
          <Layers3 size={16} />
          全部{mode === "datasets" ? "数据" : "特征"}
          <span>{all.length}</span>
        </button>
        {mode === "datasets"
          ? sources.map((s) => (
              <button
                key={s.id}
                className={group === s.id ? "selected" : ""}
                onClick={() => setGroup(s.id)}
              >
                {s.kind === "document" ? (
                  <FileText size={16} />
                ) : (
                  <Database size={16} />
                )}
                <div>
                  {s.name}
                  <small>{kindLabel[s.kind] ?? s.kind}</small>
                </div>
                <span>
                  {datasets.filter((d) => d.source_id === s.id).length}
                </span>
              </button>
            ))
          : [
              { id: "table", label: "统计与关联" },
              { id: "text", label: "文本特征" },
            ].map((g) => (
              <button
                key={g.id}
                className={group === g.id ? "selected" : ""}
                onClick={() => setGroup(g.id)}
              >
                <GitBranch size={16} />
                {g.label}
              </button>
            ))}
      </aside>
      <div className="catalog-content">
        <div className="catalog-search">
          <Search size={17} />
          <Input
            aria-label="搜索目录"
            placeholder="按名称搜索表、文档或特征"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
          <span>{visible.length} 项</span>
        </div>
        <div className="catalog-cards">
          {visible.map((item) => {
            const source = isDatasetItem(item)
              ? sources.find((s) => s.id === item.source_id)
              : null;
            const available = isDatasetItem(item)
              ? item.active_version_id
              : item.active_snapshot_id && !item.stale;
            return (
              <button
                className="catalog-card"
                key={item.id}
                onClick={() => open(item)}
              >
                <div className="catalog-card-top">
                  <span
                    className={`catalog-icon ${source?.kind === "document" ? "document" : isFeatureItem(item) ? "feature" : ""}`}
                  >
                    {source?.kind === "document" ? (
                      <FileText size={23} />
                    ) : isFeatureItem(item) ? (
                      <GitBranch size={23} />
                    ) : (
                      <Table2 size={23} />
                    )}
                  </span>
                  <span className={available ? "positive" : "pending"}>
                    {available ? "可查询" : "待更新"}
                  </span>
                </div>
                <h3>{item.name}</h3>
                <p>
                  {isDatasetItem(item)
                    ? item.source_object
                    : `${item.dependencies.length} 项输入 · ${item.entity_keys.join(", ") || "汇总特征"}`}
                </p>
                <div className="catalog-card-foot">
                  <span>{source ? kindLabel[source.kind] : "融合特征"}</span>
                  <span>
                    查看内容 <ArrowRight size={14} />
                  </span>
                </div>
              </button>
            );
          })}
        </div>
        {!visible.length && (
          <div className="surface empty">
            没有匹配的数据。试试切换分组或搜索词。
          </div>
        )}
        <div className="catalog-tip">
          <GitBranch size={17} />
          <span>从表和文档开始，查看关联特征，再带着选中的数据进入分析。</span>
        </div>
      </div>
    </div>
  );
}
