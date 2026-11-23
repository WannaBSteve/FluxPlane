# 技术指标与实验结果

FluxECR 面向文本、结构字段、历史案例和持续更新特征的联合推理。系统不把所有输入直接拼接给单一模型，而是让语义模型、结构模型、案例检索和字段规则分别产生可追踪的证据，再按目标类别进行可靠度仲裁；推理服务同时约束查询时延与特征新鲜度。

本页只报告固定协议下已经获得的结果。质量实验使用完整测试集，调参和权重选择只使用训练集与验证集；系统实验在相同资源限制下比较两种调度方式。

## 1. 技术指标

### 1.1 多模态推理与特征质量

> 在 CFPB Consumer Complaint Database 的完整时间测试集上，FluxECR 的 Macro-F1 由 Basic TF-IDF 的 **0.4785** 提升至 **0.6547**，相对提升 **36.81%**；在 Bank Account Fraud Variant V 上，特征来源门控将 TPR@5%FPR 由强 CatBoost 基线的 **0.3469** 提升至 **0.4936**，相对提升 **42.28%**。两项结果分别反映多模态推理质量和漂移环境下的特征鲁棒性，均超过约 30% 的提升幅度。

为避免只与简单模型比较，同一 CFPB 测试集还设置了 Joint Frozen E5 + CatBoost 强基线。相对该基线，FluxECR 的 Macro-F1 提升 **11.84%**，Macro-AP 提升 **0.47%**。这部分增益来自混合案例证据和类别级仲裁，而不是微调文本编码器。

### 1.2 平均响应时延

> 在隔离 Compose 的 100,000 行混合查询与特征更新负载中，动态空闲容量借用将查询平均时延由 **7,003.1 ms** 降至 **4,931.7 ms**，缩短 **29.58%**；查询 p95 同时由 **12,386.7 ms** 降至 **7,414.9 ms**，缩短 **40.14%**。

平均时延取三轮实验中“每轮查询平均时延”的中位数。固定分池与动态借用使用相同的查询、更新任务和资源限制。

### 1.3 系统资源利用率

> 在两种方式均预留 **1,000 millicores CPU** 的条件下，总吞吐由 **1.950 jobs/s** 提升至 **3.199 jobs/s**，即单位预留 CPU 的任务吞吐提升 **64.05%**。与此同时，36/36 次特征更新均在 2 秒时限内完成，说明吞吐提升没有通过牺牲特征新鲜度获得。

### 表 1　技术指标总览

| 技术指标 | 对照方法 | FluxECR | 相对变化 | 实验依据 |
| --- | ---: | ---: | ---: | --- |
| CFPB Macro-F1 ↑ | 0.4785 | **0.6547** | **+36.81%** | 完整 6,871 条时间测试集 |
| BAF Variant V TPR@5%FPR ↑ | 0.3469 | **0.4936** | **+42.28%** | 完整 241,672 条后期测试集 |
| 查询平均时延 ↓ | 7,003.1 ms | **4,931.7 ms** | **−29.58%** | 隔离 Compose，三轮 |
| 查询 p95 ↓ | 12,386.7 ms | **7,414.9 ms** | **−40.14%** | 隔离 Compose，三轮 |
| 单位预留 CPU 吞吐 ↑ | 1.950 jobs/s | **3.199 jobs/s** | **+64.05%** | 两种方式均为 1,000 millicores |
| 特征更新时限达标率 ↑ | 100% | **100%** | 保持不变 | 36/36 次更新不超过 2 秒 |

## 2. 实验设置

### 表 2　数据集、任务和评测协议

| 数据集 | 任务 | 数据划分 | 主要指标 | 回答的问题 |
| --- | --- | --- | --- | --- |
| CFPB Consumer Complaints | 使用响应前可见的投诉文本与结构字段预测 **Company response to consumer** | 按时间排序；20,610 / 6,870 / 6,871 | Macro-AP、Macro-F1 | 多模态证据能否提升少数类判断 |
| Bank Account Fraud | 在时间漂移下识别欺诈申请 | **month < 6** 训练，**month ≥ 6** 测试 | TPR@5%FPR、AUPRC | 特征来源信息能否抵抗关系漂移 |
| Mixed Query/Update | 查询与特征更新竞争两个 worker | 100k 行；每轮 20 个查询、6 个更新；三轮 | mean、p95、吞吐、时限达标率 | 动态调度能否同时改善时延与资源效率 |

CFPB 任务不是 Issue 分类。Issue 和 Sub-issue 是描述投诉内容的输入字段，预测目标是之后产生的响应类型。Company public response 和 Timely response? 等结果发生后才可见的字段不进入模型。

Macro-F1 对每个类别同等计权，适合检查少数响应类型是否被忽略；Macro-AP 衡量每个类别的排序质量；TPR@5%FPR 表示将误报率限制在 5% 时能够识别出的欺诈比例。

## 3. 多模态推理结果

### 表 3　CFPB 完整测试集上的主要结果

| 方法 | 文本表示 | 结构字段 | 案例证据 | Macro-AP ↑ | Macro-F1 ↑ |
| --- | --- | --- | --- | ---: | ---: |
| Majority | — | — | — | 0.2500 | 0.2247 |
| Basic TF-IDF + LR | TF-IDF | — | — | 0.5374 | 0.4785 |
| Structured CatBoost | — | CatBoost | — | 0.3776 | 0.2942 |
| Frozen E5 + CatBoost | Frozen E5 | — | — | 0.6375 | 0.5892 |
| Joint Frozen E5 + CatBoost | Frozen E5 | CatBoost | — | 0.6435 | 0.5854 |
| Class-wise Evidence Consensus | Frozen E5 | CatBoost + rules | — | 0.6451 | 0.6523 |
| **FluxECR** | **Frozen E5** | **CatBoost + rules** | **Hybrid Case Memory** | **0.6465** | **0.6547** |

Basic TF-IDF + LR 是容易理解的基本方法；Joint Frozen E5 + CatBoost 是更严格的强基线。FluxECR 相对 Basic 的主要提升不是由更大的可训练编码器带来：E5 在全部实验中保持冻结，新增部分是显式案例证据、字段规则和按类别学习的证据可靠度。

### 表 4　特征融合消融

| 证据配置 | Macro-AP ↑ | Macro-F1 ↑ | 相对上一行变化 |
| --- | ---: | ---: | --- |
| 仅语义案例检索 | 0.4388 | 0.3515 | — |
| 词法 + 语义 + 结构案例检索 | 0.5427 | 0.4748 | Macro-AP **+23.69%**；Macro-F1 **+35.06%** |
| Frozen E5 + 类别级证据仲裁 | 0.6451 | 0.6523 | 案例之外再融合结构与规则 |
| **完整 FluxECR** | **0.6465** | **0.6547** | 加入混合 Case Memory |

该消融说明，提升并非来自简单增加 embedding：纯 E5 相似度检索的效果有限；只有当词法相似、语义相似和 Issue、Product、Company 等结构一致性共同构成案例支持度时，检索证据才明显改善。

## 4. 漂移环境下的特征质量

### 表 5　BAF Base 与 Variant V 的时间测试结果

| 数据集 | 方法 | TPR@5%FPR ↑ | AUPRC ↑ | AUROC ↑ |
| --- | --- | ---: | ---: | ---: |
| Base | LightGBM | **0.5480** | **0.1936** | **0.8933** |
| Base | PSI Drift Gate | 0.5334 | 0.1833 | 0.8887 |
| Base | Provenance Gate | **0.5480** | **0.1936** | **0.8933** |
| Variant V | LightGBM | 0.3379 | 0.0649 | 0.7762 |
| Variant V | CatBoost | 0.3469 | 0.0644 | 0.7276 |
| Variant V | PSI Drift Gate | 0.3401 | 0.0644 | 0.7644 |
| Variant V | **Provenance Gate** | **0.4936** | **0.1457** | **0.8808** |

Variant V 中匿名字段 x1/x2 的边际分布未必剧烈变化，但它们与标签的关系已经不再可靠，因此只看 PSI 无法识别这一问题。Provenance Gate 要求关键特征具有已注册的来源和语义：它在 Base 上保持原性能，在 Variant V 上将 TPR@5%FPR 提升 42.28%，AUPRC 提升 124.48%。

这项规则是在分析 Variant V 失效模式后形成的，因此表 5 用于说明特征来源门控的作用机制，不表述为未观察数据上的独立发现。

## 5. 推理与特征更新调度

### 表 6　隔离 Compose 混合负载结果

| 调度方式 | 查询 mean ↓ | 查询 p95 ↓ | 总吞吐 ↑ | 更新 ≤ 2 s |
| --- | ---: | ---: | ---: | ---: |
| Fixed Pools | 7,003.1 ms | 12,386.7 ms | 1.950 jobs/s | 18/18 |
| **Dynamic Borrowing** | **4,931.7 ms** | **7,414.9 ms** | **3.199 jobs/s** | **18/18** |
| **相对变化** | **−29.58%** | **−40.14%** | **+64.05%** | **保持 100%** |

固定分池为查询和特征更新各保留一个 worker；动态借用允许查询在更新 worker 空闲时使用其容量，但前提是两秒更新时限仍可满足。两组实验的查询结果摘要一致，CPU 和内存预留相同，因此表中的吞吐变化可以解释为相同资源下的利用效率变化。

## 6. 大小模型协同的作用边界

### 表 7　Ling、RAG 与选择性调用

| 方法 | Macro-AP ↑ | Macro-F1 ↑ | 结论 |
| --- | ---: | ---: | --- |
| Ling only | 0.2706 | 0.3134 | 单独替换本地方法效果较弱 |
| Ling + Hybrid RAG | 0.4669 | 0.5062 | 检索证据显著改善 Ling |
| Strong Local Consensus | 0.6451 | 0.6523 | 强本地证据基线 |
| 10% 调用预算 + Supervised Router | 0.6453 | 0.6529 | 只接受 11 个外部候选，增益很小 |
| Oracle Correction | 0.7348 | 0.7657 | 候选中存在潜在修正，但当前信号难以可靠识别 |

外部模型不是默认正确的最终裁判。FluxECR 将 Ling 的结果视为新的候选证据，并允许保留本地判断。当前主要质量结果 0.6547 不依赖外部模型调用。

## 7. 结果出处

- [CFPB Basic 与线性融合基线](../docs/benchmarks/cfpb-response-agentic-local-20261122.json)
- [Frozen E5、Hybrid Case Memory 与完整 FluxECR](../docs/benchmarks/cfpb-response-hybrid-case-agentic-local-20261122.json)
- [Ling、RAG 与选择性调用](../docs/benchmarks/cfpb-response-ling-supervised-local-20261122.json)
- [BAF Base 与 Variant V](../docs/benchmarks/baf-base-variant-v-local-20261122.json)
- [隔离 Compose 混合负载](../docs/benchmarks/mixed-compose-100k-20261109.json)
- [十轮本地调度复测](../docs/benchmarks/scheduler-tuning-mean-local-20261122.json)
- [机器可读的指标摘要](results/summary.json)

完整方法流程与相邻范式的对比见 [FluxECR system figure](figures/fluxecr-system.svg)。
