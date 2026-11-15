# FluxPlane 架构导览

这张图围绕两条路径展开：数据接入后形成可持续维护的特征；用户从问题出发，分析已经发布的数据。两条路径通过共享目录、任务与版本衔接。

- [动态 HTML](fluxplane.html)：独立文件，可离线打开，支持流向动画、悬停与屏幕缩放。
- [循环 GIF](fluxplane.gif)：README 内直接播放。
- [高清 PNG](fluxplane.png)：用于说明文档和演示稿。
- [20 秒 MP4](fluxplane.mp4)：用于演示和分享。
- [Mermaid](fluxplane.mmd)：保留模块和关系，可继续编辑；排版由 Mermaid 重新计算。

## 怎么读

1. **数据接入与特征融合。** 数据库、文件、接口和文档通过连接器接入，来源变化触发特征更新。快照和特征写入 Parquet，由版本目录记录发布组合。
2. **自然语言分析。** 用户选择数据并提问，检查分析计划后确认执行。查询读取固定版本，返回表格或图表。
3. **调度与持续维护。** 共享目录保存定义、任务和版本记录。调度器安排同步与计算，后台维护查询物化结果和分区布局。

## 模块与代码

| 模块 | 主要实现 |
| --- | --- |
| 分析工作台 | `apps/web/src/components/AnalysisWorkspace.tsx` |
| 数据目录与特征浏览 | `apps/web/src/components/DataExplorer.tsx` |
| 分析规划与字段检查 | `packages/fluxplane/api/exploration.py` |
| 请求、权限与事件接口 | `packages/fluxplane/api/app.py` |
| 查询执行 | `packages/fluxplane/execution/query.py`，`application/service.py::_query` |
| 任务调度与资源准入 | `packages/fluxplane/scheduling/jobs.py`、`scheduling/policy.py` |
| 目录与任务记录 | `packages/fluxplane/storage/models.py` |
| 发布版本与历史恢复 | `packages/fluxplane/application/service.py` |
| 数据与特征产物 | `packages/fluxplane/storage/artifacts.py` |
| 数据接入 | `packages/fluxplane/connectors/`，`application/service.py::_sync` |
| 特征计算 | `packages/fluxplane/features/`，`application/service.py::_refresh` |
| 后台优化 | `application/service.py::_materialize`、`_organize` |

实际部署包括 Web、API、scheduler、查询与后台 worker，以及目录数据库。

## 更新图

模块内容和坐标集中在 `build-spec.mjs`，文字样式在 `theme.css`，生成的 `fluxplane.spec.json` 是绘图输入。安装 `arch-diagram` 后运行：

```bash
node architecture/render.mjs
export CHROME=/path/to/chrome
SKILL="$HOME/.codex/skills/arch-diagram"
node "$SKILL/scripts/check.mjs" architecture/fluxplane.html /tmp/fluxplane-check.png
node architecture/export.mjs architecture/fluxplane.html \
  architecture/fluxplane.png architecture/fluxplane.mp4 architecture/fluxplane.gif \
  architecture/fluxplane.mmd --seconds 20
```

可以用 `ARCH_DIAGRAM_SKILL` 指定技能目录。视频导出需要 ffmpeg；本项目的导出脚本逐帧捕获完整画布，避免长图在录屏时裁切。HTML 通过 `render.mjs` 构建，并保留渲染器的版权声明。

渲染器来自 [bassnova42/arch-diagram-skill](https://github.com/bassnova42/arch-diagram-skill)，使用 [MIT 许可证](licenses/arch-diagram-MIT.txt)。
