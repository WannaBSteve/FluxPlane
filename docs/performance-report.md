# 本地性能测量

> 多模态算法、特征质量和自适应调度的正式结果见
> [技术指标与实验结果](../experiments/RESULTS.md)。本页保留平台基础性能与历史负载测量。

使用 [`examples/benchmark_local.py`](../examples/benchmark_local.py) 在临时目录中生成 100 万行、四个分区值的 Parquet 数据，建立独立 SQLite 目录，执行同步、过滤聚合查询、分区布局构建与控制接口读取。查询时间包括平台任务处理；执行器时间只包括 DuckDB 查询，按原始快照与布局交替测量。每轮有五个查询样本和 100 次控制接口读取。原始结果：[第 1 轮](benchmarks/local-1.json)、[第 2 轮](benchmarks/local-2.json)、[第 3 轮](benchmarks/local-3.json)。

| 指标 | 第 1 轮 | 第 2 轮 | 第 3 轮 |
| --- | ---: | ---: | ---: |
| 数据同步 | 309 ms | 360 ms | 264 ms |
| 查询任务中位耗时，原始快照 | 167 ms | 146 ms | 274 ms |
| 查询任务中位耗时，分区布局 | 117 ms | 360 ms | 139 ms |
| 查询任务中位耗时比 | 1.44× | 0.40× | 1.97× |
| 执行器中位耗时比 | 1.38× | 1.51× | 1.69× |
| 布局构建及验证 | 3.35 s | 2.86 s | 2.89 s |
| 控制接口 p95，进程内客户端 | 9.13 ms | 7.95 ms | 9.23 ms |

原始快照为 1.46 MB，分区布局约 2.81 MB，额外布局约为原快照的 1.93 倍。三轮执行器查询均有收益，但完整查询任务受共享主机的存储与调度波动影响，第 2 轮出现回退。因此这组数据支持“过滤扫描可受益”，尚不足以宣称稳定的端到端加速。按各轮执行器中位耗时差估算，需约 53–106 次后续匹配查询才能收回一次布局构建时间，尚未计入额外存储成本。

该测量使用单进程、SQLite 元数据和进程内 HTTP 客户端，不代表 Compose 部署的网络延迟、并发负载或后台刷新时的尾延迟。运行机器报告 80 个逻辑 CPU，Python 3.13.11、DuckDB 1.5.6、PyArrow 24.0.0。复测命令：

```bash
.venv/bin/python examples/benchmark_local.py --rows 1000000 --output docs/benchmarks/local-new.json
```

## 追加更新与特征刷新

[`examples/benchmark_incremental.py`](../examples/benchmark_incremental.py) 在隔离目录中为同一数据集创建追加聚合特征和全量计算对照。每种追加比例独立运行三次，并逐行核对两个特征结果。下表是任务处理的中位耗时；[10 万行原始结果](benchmarks/incremental-100k-final.json)和[100 万行原始结果](benchmarks/incremental-1m-final.json)同时记录了 CPU 时间、内存峰值和写入字节数。

| 初始行数 | 追加量 | 实际选用路径 | 选用路径刷新 | 全量对照刷新 |
| ---: | ---: | --- | ---: | ---: |
| 10 万 | 1% | 增量 | 101 ms | 80 ms |
| 10 万 | 10% | 增量 | 92 ms | 86 ms |
| 10 万 | 50% | 全量回退 | 104 ms | 132 ms |
| 100 万 | 1% | 增量 | 104 ms | 118 ms |
| 100 万 | 10% | 增量 | 127 ms | 129 ms |
| 100 万 | 50% | 全量回退 | 102 ms | 143 ms |

当前追加同步仍读取并发布完整来源快照，增量只作用于特征刷新。百万行下小比例追加的刷新收益较小；10 万行下增量甚至略慢。50% 追加两列都执行全量计算，其差异反映共享主机波动，不是算法收益。系统因此在追加量超过新版本四分之一时回退全量计算。该阈值是保守的首版规则，需结合更大、更复杂的特征与混合负载继续校准。

## 查询与特征更新混合负载

[`examples/benchmark_mixed_repeats.py`](../examples/benchmark_mixed_repeats.py) 在同一进程内用两个 worker 线程进行三轮对照。每轮先提交 20 个查询，50 ms 后开始每隔 250 ms 提交一个特征刷新，共 6 个。四种策略依次为共享 FIFO、固定分池、后台立即借给查询，以及后台空闲 500 ms 后才借给查询。每种策略使用相同的两个 10 万行数据集，结果哈希一致；执行顺序逐轮轮换。[三轮原始结果](benchmarks/mixed-stream-grace-repeats-100k.json)记录每项任务的完成时间。下表的 p95、吞吐和内存为三轮中位数，时限达标数汇总三轮全部 18 次刷新。

| 调度方式 | 查询 p95 | 刷新 ≤ 2 s | 最后一项刷新完成时间 | 总吞吐 | 峰值 RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| 共享 FIFO | 3.36 s | 1/18 | 4.48 s | 4.23 任务/s | 345 MiB |
| 固定分池 | 5.81 s | 18/18 | 1.37 s | 4.27 任务/s | 336 MiB |
| 立即借用 | 3.68 s | 18/18 | 0.97 s | 6.78 任务/s | 338 MiB |
| 空闲 500 ms 后借用 | 4.79 s | 18/18 | 1.15 s | 5.27 任务/s | 328 MiB |

“最后一项刷新完成时间”也取每轮最慢刷新延迟的中位数。共享 FIFO 在查询积压时错过多数刷新时限；固定分池保持更新及时，但查询尾延迟较高。立即借用在这批三轮中表现较好，但[前一批三轮](benchmarks/mixed-stream-immediate-pilot-100k.json)只有 16/18 次刷新达标。默认的 500 ms 观察期为新到达的后台任务留出机会，代价是查询可能更晚借到空闲 worker。现有测量仍受单进程线程竞争和主机波动影响，不能作为容器部署的稳定性能结论。

复测命令：

```bash
.venv/bin/python examples/benchmark_mixed_repeats.py --rows 100000 --queries 20 --features 6 --late-features 6 --late-delay-ms 50 --late-spacing-ms 250 --borrow-grace-ms 500 --deadline-ms 2000 --rounds 3 --output docs/benchmarks/mixed-stream-new.json
```

### 容器部署中的混合负载

[`examples/benchmark_compose_mixed.py`](../examples/benchmark_compose_mixed.py) 在隔离 Compose 部署中通过 HTTP API 提交任务，由独立的查询和后台 worker 执行。每组使用新的 10 万行 Parquet 数据集，先提交 20 个不同的窗口聚合查询，再按 250 ms 间隔提交 6 个特征刷新。固定分池让后台 worker 保持专用；动态模式在后台空闲 500 ms 后允许其帮助查询。三轮交替运行顺序，所有查询结果哈希一致。[完整任务记录](benchmarks/mixed-compose-100k-20261109.json)保留每个任务的排队至完成时间及资源观测。

| 模式 | 查询任务 p95 | 刷新 ≤ 2 s | 最慢刷新完成时间 | 总吞吐 | 单 worker 峰值 RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| 固定分池 | 12.39 s | 18/18 | 1.36 s | 1.95 任务/s | 321 MiB |
| 空闲借用 | 7.41 s | 18/18 | 1.51 s | 3.20 任务/s | 328 MiB |

表中除达标次数外均为三轮中位数。两种模式观测到的 CPU 预留峰值均为 1,000 millicores、内存预留峰值均为 1,024 MiB；RSS 是任务采样到的单 worker 进程峰值，不代表所有容器内存之和。该负载下空闲借用缩短查询排队并提高吞吐，同时刷新仍满足时限；各轮查询 p95 波动较大，结果限定于本次 10 万行、两 worker 的部署条件。

在运行中的独立 Compose 项目根目录，使用管理员密钥和已安装的 Python 依赖复测：

```bash
.venv/bin/python examples/benchmark_compose_mixed.py --rounds 3 --output docs/benchmarks/mixed-compose-new.json
```

脚本会暂时停止 scheduler 和常驻后台 worker，分别启动两种调度配置的临时后台 worker，结束时恢复原服务。

## 容器部署中的数据规模

[`examples/benchmark_compose_scale.py`](../examples/benchmark_compose_scale.py) 在隔离 Compose 实例中为 1 万行和 100 万行分别创建三个新数据集。事实表含确定性的高基数字段；后台 worker 同步文件并发布聚合特征，查询 worker 将事实表与演示商品维表连接，实际读取高基数字段。每组运行五条不同的跨源查询，查询与特征结果均和独立计算值一致。[逐任务原始结果](benchmarks/compose-scale-20261109.json)同时保存输入文件、发布快照和特征快照的大小。

| 每组行数 | 来源文件 | 同步任务 | 特征刷新 | 查询任务 p50 | 首次答案 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 万 | 0.136 MB | 259 ms | 364 ms | 1.46 s | 2.30 s |
| 100 万 | 10.408 MB | 617 ms | 446 ms | 1.54 s | 3.03 s |

耗时为三组中位数，均按任务提交到完成计算。首次答案从提交来源同步任务计时，经过特征注册、刷新，直到第一条跨源查询完成；生成测试文件的时间单独记录，未计入该值。两档发布的数据快照分别约 0.136 MB 和 10.407 MB，聚合特征快照均约 1.2 KB。这里的文件大小是压缩后的输入与产物大小，不能代替执行器实际扫描字节数。数据量增大时同步和首次答案耗时增加，查询任务耗时变化较小；这组结果只说明该部署条件下的实际表现，不推断普遍扩展曲线。

在已运行完整多源演示的独立 Compose 项目中复测：

```bash
.venv/bin/python examples/benchmark_compose_scale.py --sizes 10000 1000000 1000000 10000 10000 1000000 --output docs/benchmarks/compose-scale-new.json
```

脚本在测量期间暂停文件自动扫描，来源同步、特征刷新和查询仍由实际 worker 完成；结束时恢复 scheduler。
