# 部署、备份与恢复

## 部署与升级

在独立目录复制仓库，按 `.env.example` 设置数据库密码、管理员密钥和 Web 端口。运行 `docker compose up --build -d` 后，访问 `/health/ready` 或控制台；API、调度器和 worker 启动时会执行 Alembic 迁移。升级前先完成下面的备份，再替换代码并重新构建。元数据库和 `runtime/artifacts`、`runtime/sources` 必须一起迁移。

## 一致备份

以下命令在 Compose 项目目录执行。先停止写入服务，保持 PostgreSQL 运行，再导出目录和不可变产物：

```bash
docker compose stop web api scheduler worker-query worker-background
mkdir -p backup
docker compose exec -T postgres pg_dump -U fluxplane -d fluxplane -Fc > backup/catalog.dump
tar -C runtime -cpf backup/runtime.tar artifacts sources
docker compose up -d
```

备份文件应放在 Compose 项目目录之外长期保存；这里的 `backup` 只是本地暂存路径。外部 PostgreSQL 或 HTTP 来源的数据由其自身备份机制管理，平台备份保存的是已经发布的快照。

## 在新目录恢复

在一个**空白**的 Compose 项目目录中放入与备份兼容的代码、`.env`、`catalog.dump` 和 `runtime.tar`。先恢复文件和元数据库，再检查引用与校验和，最后启动服务：

```bash
mkdir -p runtime
tar -C runtime -xpf backup/runtime.tar
docker compose up -d postgres
docker compose exec -T postgres pg_restore -U fluxplane -d fluxplane --no-owner --clean --if-exists < backup/catalog.dump
docker compose run --rm --no-deps api fluxplane verify-storage
docker compose up -d
```

`verify-storage` 会逐项检查已引用的快照、特征、查询结果及分区布局；有缺失或校验和不符时返回非零退出码。部署目录若变化，容器内产物路径仍需保持为 `/data/artifacts`。旧版本的数据库迁移应先在备份副本中试运行。

2026-11-09 的隔离恢复演练使用两个全新 Compose 项目。源实例发布了四类来源、跨源特征、查询物化和分区布局；按上述步骤恢复后，`verify-storage` 检查通过 21 个产物及 1 个布局。新实例的跨源报表、物化命中、布局过滤查询、Web 页面和健康检查均通过。

## 独立启动与服务重建验证

另从源码克隆到独立目录，用单独的 Compose 项目、元数据卷、来源和产物目录启动完整服务。`seed`、`optimize`、`update`、`verify` 四阶段多源演示通过，Web 与 API 可访问；更新后的 `verify-storage` 核对了 95 个已引用产物。实际查询积压时，后台 worker 借用了查询任务；随后提交的特征刷新由该 worker 完成，完成时仍有 38 个查询未结束。

完整演示中的 `schema-guard` 阶段需暂时停止 scheduler，避免自动文件扫描与故障注入同时提交同步任务。该阶段移除 Parquet 来源的一个必需字段，检查任务失败、活跃 release 和数据版本保持不变、既有查询仍可用，随后恢复原文件并重新同步。隔离实例中的任务实际报错为 `incompatible schema change: category`；恢复后完整报表通过，`verify-storage` 核对 101 个产物无异常。运行方式见 [README](../README.md#完整多源演示)。

同一实例还重复运行了 `restore` 阶段：更新前 release 的配件净收入为 23,000 分，恢复更新后的 release 后为 27,000 分，其他报表和特征检查均通过。脚本离开时保留更新后的状态；[切换记录](benchmarks/release-restore-20261109.json)列出原 release、新发布的 release、订单来源更新时间水位及两份报表。

后台 worker 故障演示在同一隔离项目中运行两次。每次的同步任务均出现一次租约过期、一次成功重试；数据版本和 release 各只增加 1，发布后查询返回新值。脚本恢复了 scheduler 和 worker；随后 `verify-storage` 核对 105 个产物无异常。[第二次运行的原始结果](benchmarks/worker-recovery-20261109.json)保留了任务、版本和 release 标识。隔离演示使用 10 秒租约，普通部署默认 120 秒。

同一隔离部署还完成三轮通过 HTTP API 提交的查询与特征刷新混合负载。固定分池与后台空闲借用的结果哈希一致，运行后 scheduler 与常驻 worker 均已恢复；`verify-storage` 核对 304 个产物无异常。任务级原始数据和复测方式见[性能报告](performance-report.md#容器部署中的混合负载)。

随后用高基数字段的事实表完成 1 万行和 100 万行的三组部署规模测量。每组均通过特征与跨源查询的独立结果检查；scheduler 已恢复，`verify-storage` 核对 423 个产物无异常。[规模测量结果与复测方式](performance-report.md#容器部署中的数据规模)记录了同步、刷新、查询和存储大小。

本地反向代理的 100 次小型控制请求 p95 为 16.64 ms；空闲条件下 20 个顺序提交查询的派发延迟 p95 为 318.76 ms。[部署探测原始结果](benchmarks/isolated-compose-20261109.json)记录健康、目录规模、执行池及控制请求耗时。随后只重建 API 容器而保持 Web 容器不变，Web 健康检查、认证 API 与 SSE 连接均恢复为 200；这一场景验证了代理在服务地址变化后重新解析 API。

完成演示后，可从仓库根目录复测 Web 和 API：

```bash
.venv/bin/python examples/verify_deployed.py --env-file .env --expect-active-materialization
```
