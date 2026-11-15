# FluxPlane 功能与运行细节

这里集中记录开发启动、接口行为、更新策略和运行边界。首次体验请从[仓库首页](../README.md)和[案例演示](demo.md)开始。

## 本地运行

建议使用 Python 3.13（与 [Python 依赖锁文件](../requirements.lock)及 API 镜像一致）、Node.js 24+ 和 npm。以下命令在仓库根目录执行，使用 SQLite 元数据库：

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.lock
.venv/bin/pip install -e '.[dev]'
mkdir -p runtime/sources runtime/artifacts
export FLUXPLANE_DATABASE_URL="sqlite:///$PWD/runtime/metadata.sqlite3"
export FLUXPLANE_SOURCE_ROOT="$PWD/runtime/sources"
export FLUXPLANE_ARTIFACT_ROOT="$PWD/runtime/artifacts"
.venv/bin/python examples/bootstrap_demo.py
```

演示脚本注册 CSV、SQLite 和 Markdown 来源，发布数据集和客户消费特征，并执行跨源查询。启动服务时，在四个终端复用上述环境变量：

```bash
.venv/bin/fluxplane api --port 8040
.venv/bin/fluxplane worker --lane query
.venv/bin/fluxplane worker --lane background
.venv/bin/fluxplane scheduler
```

另在 `apps/web` 执行 `npm ci && npm run dev`，打开 `http://127.0.0.1:5173`。开发服务器把 `/api` 转发到本地 API。生产部署应设置管理员密钥 `FLUXPLANE_API_KEY`，并在控制台输入该密钥；可另设不同的 `FLUXPLANE_OPERATOR_API_KEY` 和 `FLUXPLANE_VIEWER_API_KEY`。HTTP 来源还需由实例管理员设置 `FLUXPLANE_HTTP_ALLOWED_HOSTS`，多个主机以逗号分隔。

只读密钥可查看目录、版本、任务与查询结果；操作员还可提交同步、刷新、查询和执行计划，取消或重试任务；管理员另可注册来源与定义、恢复发布版本。控制台使用 `GET /api/v1/auth/me` 显示当前角色，并按相同权限显示操作。`quickstart.sh` 创建的本机演示实例不启用密钥；普通 Compose 启动有默认管理员密钥，正式部署须在 `.env` 中替换默认值。当前权限作用于单个实例，尚无用户账户或按数据集划分的多租户授权。

## 发布与特征更新

每次数据或特征发布会形成一个 release。查询绑定提交时的 release；控制台“版本与存储”可查看历史并创建恢复发布。存储页显示超过一小时且未被引用的孤儿产物。CLI 的 `.venv/bin/fluxplane gc` 只预览；确认后使用 `.venv/bin/fluxplane gc --apply` 清理。清理期间若有运行任务，命令会拒绝执行。已发布版本与查询结果产物不会被该命令删除。

特征中心提供 SQL 全量计算、跨源事件窗口 SQL、追加数据增量聚合、单源事件时间滚动聚合及注册的 Python 算子。跨源事件窗口可为多个输入数据集分别声明事件时间字段与窗口长度，其余输入保持完整快照；同一次刷新使用统一的时间点过滤输入、执行特征 SQL，并记录下一次事件进入或退出窗口的时刻。内置 `text_match_count` v1 对单个数据集的文本字段按行统计指定字符串的出现次数；定义保存算子版本和参数，可在控制台创建，也可查看 `GET /api/v1/feature-operators`。刷新使用已发布的输入版本，结果与 SQL 特征经过相同的实体键校验和发布流程。它执行平台预装代码，不接受上传或提交任意 Python 代码。追加聚合针对单一数据集、实体键、求和字段和计数生成受限 SQL；只有新快照经逐行前缀比对确认为追加、且追加量不超过新版本四分之一时才使用增量特征计算；较大追加、改写或删除会完整重算。来源同步目前仍读取完整快照，增量优化针对特征计算阶段。

随时间变化的 SQL 特征可设置 `refresh_interval_seconds`，例如滚动窗口结果每 60 秒重算一次。调度器到期后提交刷新，即使来源版本未变也会重新计算；到期但尚未刷新时，查询会返回明确的过期错误，控制台也会标记特征。该间隔是周期性新鲜度设置，尚不按每条事件的精确窗口退出时刻调度。

单源求和/计数特征可选择 `rolling_sum`，声明事件时间字段和 `window_seconds`。跨源特征可选择 `window_sql`，对需要窗口过滤的输入逐一声明同样的规则，SQL 则在过滤后的输入上计算。两种方式都记录下一条事件进入或离开窗口的时间；调度器到达该边界后刷新，期间查询会明确报告过期。没有待发生边界时，特征等待来源变化再刷新。调度器按配置的扫描间隔发现到期事件，因而实际刷新可能晚于边界。`window_sql` 的时间依赖应来自声明的窗口；注册时会拒绝 `CURRENT_TIMESTAMP`、`NOW` 等动态函数。

调度进程连续观察文件和文档来源；同一文件状态在连续两次扫描中保持稳定且内容与活动版本不同时，自动提交同步任务。数据库和 HTTP 来源按 `FLUXPLANE_REMOTE_POLL_SECONDS` 定期同步，同一数据集已有待处理或运行任务时不会重复提交。PostgreSQL 时间游标来源还会按 `FLUXPLANE_POSTGRES_FULL_RECONCILE_SECONDS` 定期全量对账。

## 调度与运行观测

任务启动前会预留 CPU 与内存预算，跨交互和后台执行池统一计算；容量不足时保持等待，单个任务的预留超过实例预算时明确失败。预算由 `FLUXPLANE_RESOURCE_CPU_MILLICORES` 和 `FLUXPLANE_RESOURCE_MEMORY_MB` 设置，任务中心显示当前预留量，分析工作台可以调整单次查询的预留。后台 worker 无任务可领并持续空闲 500 ms 后可帮助处理查询，观察期由 `FLUXPLANE_BACKGROUND_BORROW_GRACE_MS` 调整；已借出的查询不会被中断。这里的资源数字是调度准入额度，不是强制资源上限或实际使用量。

任务完成后还会记录 worker 进程的实际 CPU 时间和运行期间采样到的最高常驻内存（RSS）；任务详情显示本次观测，总览汇总近一小时的任务，并标明有资源实测值的样本数。内存每 25 ms 采样一次，短于采样间隔的峰值可能未被捕获。多线程查询的 CPU 时间可能大于墙钟耗时；多个任务共用一个进程时，这些指标代表进程观测，不能视为单任务独占的资源用量。

## 查询优化

稳定的只读 SQL 在相同输入版本上重复执行后，系统会安排后台物化。当前触发条件是至少 3 次查询且累计耗时至少 30 ms；构建时再次执行并核对结果与输入版本。后续相同 SQL 可复用物化结果，来源更新后自动按新版本重新观察。控制台“版本与存储”显示构建时间、产物大小、复用次数和估计净收益。估计净收益以物化前同一 SQL 的平均执行时间为基线，减去命中查询的实测总耗时与构建时间；它是运行期估计值。调度器每分钟检查物化结果，淘汰超过 `FLUXPLANE_MATERIALIZATION_STALE_SECONDS` 的非当前输入结果，并将活跃物化产物总量控制在 `FLUXPLANE_MATERIALIZATION_MAX_BYTES` 内；有任务运行时延后回收。恢复旧数据版本后，热点查询可以重新物化。物化目前只覆盖受限的确定性函数，不处理动态时间查询。

对于至少 1 万行、最近一小时内被同一字段等值过滤至少 20 次的数据集，调度器会尝试建立按过滤字段划分的物理布局；管理员也可在数据目录中手动提交。后台先合并分区片段，再核对全部数据和过滤结果，并比较原始快照与新布局的过滤时间和空间占用。只有通过检查的布局才会用于匹配该字段及数据版本的查询；数据版本更新后自动回到新快照。控制台显示布局状态、探针耗时和使用次数。

## 来源接入

PostgreSQL 来源可在注册时同时指定 `primary_key` 与 `updated_at`（时间戳列），并可指定 `deleted_column` 作为软删除标记。HTTP JSON 来源也可指定 `primary_key`、`updated_at`（带时区的 ISO 时间戳）和可选的 `deleted_column`、`cursor_param`；`field_map` 可将来源字段名映射为目录中的 SQL 字段名，例如 `{"orderId":"order_id"}`，游标相关列填写映射后的名称。增量接口须按传入的 `updated_after`（或指定的参数名）返回更新时间不早于该值的记录。首次同步读取快照，之后从上次版本的时间游标向前重叠 60 秒读取变更，按主键合并并发布新版本。来源须保证更新时间随修改前进；硬删除及超出重叠范围的迟到提交由定期全量对账发现，也可调用 `POST /api/v1/datasets/{id}/sync?full=true`。对账周期分别由 `FLUXPLANE_POSTGRES_FULL_RECONCILE_SECONDS` 和 `FLUXPLANE_HTTP_FULL_RECONCILE_SECONDS` 配置。来源游标与数据版本在同一事务中发布。没有游标配置的 HTTP 来源仍按同源 `next` 分页读取完整快照。

HTTP 响应默认读取 `data` 或 `items` 列表及 `next` 下一页地址。其他 JSON 结构可设置点分隔的 `records_path` 和 `next_path`，例如 `payload.records` 与 `paging.next`；下一页仍必须与初始 URL 同源。

元数据使用 Alembic 迁移。`fluxplane init`、API、worker 和 scheduler 启动时自动升级；部署升级前应备份元数据库，也可先运行 `fluxplane migrate`。已有原型库只有在表结构与初始版本一致时才会被接管，未知结构会拒绝自动标记。

## 容器部署

仓库提供 [Compose 配置](../compose.yaml)、API 和 Web 镜像定义。首次体验直接运行 `bash scripts/quickstart.sh`，脚本生成本地配置并装载完整演示数据，无需输入密钥。Web 默认只绑定本机 `127.0.0.1:8088`。对外部署或接入自己的数据时，复制 `.env.example` 为 `.env`，设置数据库密码和 API 密钥，再运行 `docker compose up --build -d`；控制台使用该密钥访问实例。

部署目录决定 `runtime/artifacts` 与 `runtime/sources` 的绑定路径。迁移部署时须同时迁移元数据库卷和这两个目录，不能只保留数据库卷；已发布版本在元数据库中引用对应的不可变产物路径。

### 完整多源演示

可选 `demo` profile 会启动独立的 PostgreSQL 订单库和 HTTP 退款源。先在 `.env` 中设置 `FLUXPLANE_HTTP_ALLOWED_HOSTS=demo-source`，再运行：

```bash
docker compose --profile demo up --build -d
docker compose exec api python examples/full_demo.py --phase seed
docker compose exec api python examples/full_demo.py --phase optimize
docker compose exec api python examples/full_demo.py --phase update
docker compose stop scheduler
docker compose exec api python examples/full_demo.py --phase schema-guard
docker compose exec api python examples/full_demo.py --phase restore
docker compose start scheduler
docker compose exec api python examples/full_demo.py --phase verify
```

演示同时接入 PostgreSQL 订单、Parquet 商品、HTTP JSON 退款及 Markdown/PDF 文档。各阶段的结果和控制台观察位置见[案例指南](demo.md)；[版本切换记录](benchmarks/release-restore-20261109.json)提供报表数值。重新运行 `seed` 会重置演示来源，请在独立实例中使用。

### Worker 故障恢复演示

在**隔离的** Compose 项目中完成 `seed` 后，将 `.env` 中的 `FLUXPLANE_WORKER_LEASE_SECONDS` 设为 `10` 并重新创建服务。确认后台任务已完成，然后从该项目根目录运行：

```bash
python examples/worker_recovery_demo.py
```

脚本会暂时停止 scheduler，并在后台 worker 领取同步任务后停止该 worker。租约到期后，它重启 worker，检查任务重试、数据可查询，以及只发布一个数据版本和一个 release；结束时恢复服务。独立实例的[运行记录](benchmarks/worker-recovery-20261109.json)可供对照。使用独立 Compose 项目名时传入 `--project`；没有 Compose 插件时，`--compose` 可指定独立二进制路径。

## 验证

运行 `.venv/bin/pytest -q`、`.venv/bin/ruff check .` 和 `cd apps/web && npm run build && npm run check:api`。测试范围与部署核验记录见[交付验收](delivery-audit.md)。
