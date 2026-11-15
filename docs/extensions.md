# 扩展接口

FluxPlane 首版使用显式注册的来源类型和特征算子。扩展代码随 API、scheduler 和 worker 镜像一起部署；当前没有运行时插件加载或执行用户上传代码的机制。

## 新增数据来源

1. 在 [`Platform.add_source`](../packages/fluxplane/application/service.py) 中增加来源类型、配置字段及注册时校验。凭据保存为环境变量引用，不写入目录记录。
2. 在 [`connectors/snapshot.py`](../packages/fluxplane/connectors/snapshot.py) 中实现快照读取，返回 `SourceSnapshot(table, fingerprint, source_cursor, mode)`。`table` 是有稳定 schema 的 Arrow 表；`fingerprint` 标识此次读取的来源内容。只有来源能提供可靠主键与更新游标时，才实现变更读取。
3. 将新类型接入 `Platform._sync`。读取结束后沿用现有的 schema 检查、不可变 Parquet 写入、租约校验和事务发布路径；来源代码不能直接修改活动 release。
4. 若需要定时轮询，将来源加入 `Platform.poll_remote_sources` 或文件稳定性扫描。控制台的注册表单位于 [`apps/web/src/App.tsx`](../apps/web/src/App.tsx)。
5. 增加快照、重复同步、变更合并、删除、schema 变化和失败后旧版本仍可读的测试。带游标的来源还需覆盖迟到更新与全量对账。

HTTP JSON 的 `field_map` 展示了来源字段向目录字段的转换位置；`primary_key`、`updated_at` 和 `deleted_column` 使用转换后的字段名。`records_path` 和 `next_path` 指向响应中记录列表与下一页地址；游标参数仍由上游 API 定义。

## 新增特征算子

在 [`features/operators.py`](../packages/fluxplane/features/operators.py) 中注册算子的名称和版本，并实现配置校验与 Arrow 输入到 Arrow 输出的计算。定义必须声明依赖数据集与实体键；输出按实体键验证后，通过现有刷新任务发布 `FeatureSnapshot`。在 `Platform.add_feature` 和 `Platform._refresh` 接入新的算子类型，再为控制台增加相应的表单。升级算子语义时使用新版本号，使已有定义的计算含义保持稳定。

算子仅运行平台预装的代码。需要外部模型或服务时，应明确超时、失败与结果版本语义，再通过后台任务接入，不让外部调用绕过任务状态和发布检查。

## 新增后台任务

任务在 [`scheduling/jobs.py`](../packages/fluxplane/scheduling/jobs.py) 中提交和领取，并由 `Platform.process` 分派到执行函数。新增任务需明确执行池、优先级和 CPU/内存预留；产物先落盘，随后检查租约代次及输入版本，在一个元数据事务中发布结果和完成状态。失败或取消后的临时产物由孤儿产物回收处理。需要前置任务时使用持久化的 `JobDependency`，避免仅在进程内保存依赖。

部署新代码前运行迁移与测试；备份和恢复步骤见[部署文档](operations.md)。
