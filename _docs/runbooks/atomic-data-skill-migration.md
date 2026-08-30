---
docType: runbook
scope: repo
status: current
authoritative: true
owner: skills
language: zh-CN
whenToUse: "盘点、选择、薄化和验证数据 fetch/search/download Skills 时。"
whenToUpdate: "当迁移候选、批次、CLI 版本依赖、PR 顺序、删除范围或验收命令变化时。"
checkPaths:
  - AGENTS.md
  - _docs/architecture/atomic-data-capabilities.md
  - _docs/contracts/repo-contract.md
  - README.md
  - README.zh-CN.md
  - "*-fetch/**"
  - "*-search/**"
  - "*-download/**"
  - tiangong-auto-research/**
lastReviewedAt: 2026-08-30
lastReviewedCommit: ff35b50
---

# 原子数据 Skill 迁移实施计划

## 当前基线

- 计划基线：`origin/main` at
  `4104e527facd09ecc242dad7a1e9645adf9d21f0`。
- 计划分支：`codex/atomic-data-plan`，使用独立干净 worktree。
- 原 Skills checkout、现有 `codex/atomic-environment-data-skills` worktree 及 CLI 的
  未提交变更都保持不动，不 stash/reset/rebase/clean。
- 首批实现继续使用该独立 worktree；正式 CLI 包、binding 和全部门禁通过后才提交 PR。

## 2026-08-30 实现状态

- CLI PR #71 已合并到 `tiangong-ai/cli` 主分支，merge commit 为 `832e302`；
  TypeScript 7、data runtime、AirNow、Federal Register 以及 Execution Manifest /
  Discovery Metadata 分层均已进入源码主线。
- 当前可安装的 `@tiangong-ai/cli@0.0.53` 尚不包含 `data` 命令，因此仍未达到删除
  Skill 旧执行脚本和提交正式 binding 的门槛。
- Skills 仓库已增加 execution-only binding 生成/校验器及离线 stale-binding 测试。
  AirNow、Federal Register、USGS Water IV 和 Open-Meteo Air Quality 已在本地候选
  分支薄化；前两项已通过 copy/symlink 安装 smoke，后两项使用对应新增 TS7 connector
  的本地候选包完成 binding 后纳入同一 smoke。四项旧 Python connector 与重复 provider
  references 已移出候选 Skill。
- 候选包版本不得冒充正式发布版本。PR 前必须用实际包含全部四个 connector 的正式
  版本重新生成四个 binding，并用该 npm 包重跑全部门禁。

## 与 CLI 的同步顺序

1. CLI 与 Skills 计划 PR 同时开放，先确认两仓所有权、命令候选、迁移范围和验收。
2. CLI 独立完成 TypeScript 7 基线 PR。
3. CLI 完成空 data runtime/机器 contract PR，再实现首批 connectors。
4. CLI 发布候选版本，导出 canonical manifest/Schema digest。
5. Skills 首批迁移 PR 使用候选包做验证；正式 CLI 发布后更新 exact binding 并合并。
6. CLI 再接入 Research adapter；需要修改 Auto Research Skill 时遵循其强制 clean-room
   RED/GREEN 门禁。

因此不是“Skills 先合并、CLI 后跟进”。Skills 可以提前评审语义和迁移 diff，但 CLI
机器基座必须先确定、先可测试、先可发布。

## 全量候选清单

以下清单用于分类，不代表全部批准迁移。

### A. 原子 provider connector 候选

- `airnow-hourly-obs-fetch`
- `federal-register-doc-fetch`
- `usgs-water-iv-fetch`
- `nasa-firms-fire-fetch`
- `open-meteo-air-quality-fetch`
- `open-meteo-flood-fetch`
- `open-meteo-historical-fetch`
- `openaq-data-fetch`
- `regulationsgov-comments-fetch`
- `regulationsgov-comment-detail-fetch`
- `gdelt-doc-search`
- `gdelt-events-fetch`
- `gdelt-gkg-fetch`
- `gdelt-mentions-fetch`

### B. 需要内容/媒体/下载语义评审

- `ai-tech-rss-fetch`
- `ai-tech-fulltext-fetch`
- `eceee-news-fulltext-fetch`
- `sustainability-rss-fetch`
- `sustainability-fulltext-fetch`
- `bluesky-cascade-fetch`
- `youtube-video-search`
- `youtube-comments-fetch`
- `figshare-data-download`
- `academic-paper-download`

这组可能涉及正文提取、浏览器/下载产物、平台内容许可或既有 Research companion，不能
只因当前有脚本就自动转成普通 JSON connector。

### C. 保持现有产品边界，默认不迁入 data runtime

- `tiangong-kb-course-search`
- `tiangong-kb-course-fulltext-fetch`
- `tiangong-kb-edu-search`
- `tiangong-kb-esg-search`
- `tiangong-kb-patent-search`
- `tiangong-kb-report-search`
- `tiangong-kb-sci-search`
- `tiangong-kb-textbook-search`
- `dify-knowledge-base-search`
- `fetch-abstract-to-kb`
- `fetch-meta-from-kb`
- `fetch-meta-to-kb`

它们属于 Tiangong KB、Dify 或现有 ingest/search 产品面，应在各自命令边界演进。

### D. 单独安全评审

- `email-imap-fetch`
- `email-imap-full-fetch`

私有邮箱、账户凭证和正文处理不作为公共数据 connector 的默认迁移对象。

每次清单更新都要记录：官方来源、许可/ToS、认证、operation、输入/输出、分页/分块、
大小和速率限制、partial 语义、现有 fixture、真实使用者、维护成本、目标分类和决定。

## 首批：AirNow + Federal Register

### 选择理由

- AirNow Hourly Obs：无需凭证，覆盖有界 UTC 窗口、多文件 CSV、空间/污染物过滤、来源
  文件 lineage 和 partial file failure。
- Federal Register Documents：无需凭证，覆盖稳定 query 编码、分页 JSON、记录上限、
  官方政策元数据和“只取搜索结果、不抓正文”的边界。
- 两者共同验证两种有代表性的 transport/shape，同时避免首批评审被真实 secret 和
  provider 账户状态阻塞。
- credential/redaction 在 CLI foundation 由 synthetic connector 强制测试；NASA
  FIRMS 在下一批验证真实 logical credential 路径。

### 迁移前提

- CLI TypeScript 7 和基础 data contract 已合并。
- 两个 CLI connector 的 manifest、Schema、fixtures、错误和 receipt 测试已通过。
- 可安装的 CLI 候选包能导出 canonical binding。
- 现有 Skill 的外部行为、来源说明和限制已盘点；旧 Python 只读。

### Skill 变更

每个首批 Skill：

1. 用 `skill-creator` 工作流更新 `SKILL.md` 和生成的 `agents/openai.yaml`。
2. 新增 CLI 生成的 `references/tiangong-data-binding.json`。
3. 删除已由 CLI Discovery Metadata 发布的数据源/API/覆盖/许可/限制副本；只在确有
   任务型选择语义时保留非重复 reference。凭证解析转由 CLI 时删除重复配置。
4. 将 Python fetch 脚本、OpenClaw chaining 模板和旧 raw artifact 约定从生产路径移除。
5. 示例只使用精确 capability/operation 和文件/stdin 输入，不放 secret 到 argv/JSON。
6. README/marketplace 只有在可用性、安装或发现面实际变化时才同步更新。

### 旧实现行为对照

首批迁移按已评审的 capability v1 边界重写，不把旧 Python 命令行逐参数兼容层带入
Skill。以下差异必须明确，不能被误写成无损命令替换：

| Skill | 保留到 CLI capability 的核心行为 | 有意收敛或替代的旧行为 |
| --- | --- | --- |
| AirNow | UTC 小时文件规划、bbox/time/pollutant 过滤、站点小时记录、逐文件 lineage、缺失/坏文件 partial | 任意 endpoint/path/user-agent/重试调参、Skill `check-config`、dry-run、日志文件和 raw artifact 写入被 CLI manifest、`data doctor`、统一 limits、run-result/receipt 取代；非整点输入不再静默截断而是拒绝 |
| Federal Register | publication date、term、agency、type、topic、docket、RIN、稳定查询编码、有界分页/记录、空结果/截断/later-page partial | `section`、`significant` 输入过滤、任意 `fields` 投影和 `executive_order_number` 排序不属于首个 capability v1；Skill 不再宣称支持。每次调用的 page/record 上限改用 run-request 顶层 `limits` 且只能降低 manifest 上限；dry-run、日志和 raw artifact 路径不再保留 |
| USGS Water IV | bbox 或最多 100 个 sites、period 或显式 window、参数/site type/status/agency 过滤、WaterML series/value 归一化、qualifier/provisional、no-data 过滤和坏 row/series partial | 旧脚本允许本地 env/argv 覆盖 endpoint、重试、节流、上限、user-agent、日志和 `file://` fixture，并提供 `check-config`、dry-run、raw artifact 写入；这些改由 CLI endpoint policy、manifest limits、static doctor、fixture tests 和 run-result/receipt 取代。官方 legacy 上限把旧 Skill 的 200 sites 收紧为 100，且明确 2027-Q1 decommission 风险 |
| Open-Meteo Air Quality | 最多 10 个坐标、92 个闭合日期、16 个官方 hourly variables、domain/cell selection、单次多坐标响应、nullable aligned arrays 和坐标/变量 partial | timezone 固定为 GMT，删除任意 timezone、endpoint、API key、重试、节流、user-agent、日志、dry-run 和 raw artifact 调参。公开 endpoint 明确为 non-commercial 且无凭证；商业 customer endpoint/API key 需要独立 capability 评审。输出改为 location-hour 列式结果和统一 run-result/receipt，不兼容旧 snake_case payload |

新结果是 `tiangong.data.run-result.v1`，字段命名和审计结构以 operation output Schema
及 core receipt 为准，不承诺旧 Python payload 的 snake_case/raw-artifact 兼容。需要旧版
已收敛过滤或输出字段的消费者必须先推动新的 capability/operation 版本评审，不能绕回
已删除脚本。

### 首批验收

- binding 中的 capability、operation、minimum CLI version 和 digests 与候选/正式包一致。
- binding 只锁 Execution Manifest 和 operation Schema digest；Discovery Metadata
  文案或 `discoveryDigest` 变化不得触发 execution binding 漂移。
- `quick_validate.py <skill-path>` 和生成 agent metadata 校验通过。
- 离线 stale-binding 测试分别覆盖缺失 capability、过低 CLI 版本和 digest 漂移。
- copy/symlink 安装 smoke 使用临时 project/HOME，只运行 version、catalog、describe、
  静态 doctor 和 fixture/local dry contract；不访问真实 provider。
- 仓库中不再存在首批 provider 的第二份可执行业务逻辑。

## 后续批次

### 批次 2：时序/空间与凭证

USGS Water IV 已作为本批首项在本地完成 CLI connector 与 Skill 薄化；Open-Meteo Air
Quality 已作为第二项完成。下一项按序迁移 Open-Meteo Flood，再评审 Open-Meteo
Historical；随后是 NASA FIRMS、OpenAQ、Regulations.gov。

USGS 已扩展时间序列、空间范围、变量、qualifier 和 legacy 生命周期语义；Open-Meteo
Air Quality 已验证模型网格、GMT 列式多变量数据、public/commercial endpoint 分离和
attribution 语义。继续逐项评审 Flood/Historical，再用 NASA FIRMS/OpenAQ/
Regulations.gov 验证真实 logical credential/provider-auth 诊断。每个 connector 单独
批准，不因共享 provider 品牌而把多个 operation 合成一个巨型 Skill。

### 批次 3：GDELT 与内容/社交来源

先评估 GDELT 的 doc/events/GKG/mentions 是否应为一个 capability 的多个独立 operation，
还是保持多个能力。随后分别评审 RSS/fulltext、Bluesky、YouTube 和 Figshare 的内容许可、
下载产物与浏览器边界。`academic-paper-download` 继续保持 Research acquisition companion，
除非另一个明确设计取代它。

### 不迁移/退役

没有真实消费者、API/许可不稳定、无法获得可维护 fixture、长期失败或与既有 Tiangong
产品命令重叠的能力，可以保留、合并或退役。该决定单独记录，不以数量对等为目标。

## PR 拆分与依赖

建议序列：

1. Skills plan PR：本文、架构、docpact 路由；不改 Skill。
2. CLI plan PR：与 Skills plan 同步评审。
3. CLI TS7、foundation、pilot PRs：先合并并产出候选/正式包。
4. Skills pilot PR：可在候选包出现后以 draft 开放，正式包发布后更新 binding 并合并。
5. CLI Research adapter PR 与必要的 Auto Research Skill PR：单独 clean-room TDD。

若维护者要求每个仓库只保留一个实现 PR，仍遵守同一依赖：两边同时审阅，CLI 先合并/
发布，Skills 后合并。不得使用分支 commit 作为长期生产 pin，也不得让 CLI 发布审计依赖
尚未合并的 Skills commit。

## 验证流程

计划/治理变更：

```bash
docpact validate-config --root . --strict
docpact lint --root . --worktree --mode enforce
```

实际 Skill 变更还必须：

- 按 `AGENTS.md` 完整读取并使用 `skill-creator`；
- 对每个目标运行 `scripts/quick_validate.py <skill-path>`；
- 运行新增的 binding contract 和隔离安装 smoke；
- 运行受影响脚本/引用/agent metadata 测试；
- Auto Research 或直接 evidence wrapper 变更必须在相互独立的 clean container 中先
  观察 RED、再转 GREEN，PR 前运行 cold gate。

binding 工具的离线契约测试：

```bash
node --test scripts/tests/data-skill-binding.test.mjs
```

隔离 copy/symlink 安装 smoke：

```bash
TIANGONG_DATA_SKILLS_RUN_INSTALL_SMOKE=1 \
TIANGONG_DATA_CLI_VERSION=X.Y.Z \
TIANGONG_DATA_CLI_PACKAGE=@tiangong-ai/cli@X.Y.Z \
node --test scripts/tests/data-skill-install-smoke.test.mjs
```

它会清除 provider 凭证，只运行 version、catalog、describe、static doctor 和本地
结构化阻断请求，不访问 AirNow、FederalRegister.gov、Open-Meteo 或 USGS
WaterServices。

正式 CLI 版本发布后，先生成再用同一精确包复验；`X.Y.Z` 必须替换为实际可安装且
包含 `data` 命令的版本：

```bash
node scripts/data-skill-binding.mjs generate \
  --skill airnow-hourly-obs-fetch \
  --capability airnow.hourly-observations \
  --operations fetch-hourly \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs generate \
  --skill federal-register-doc-fetch \
  --capability federal-register.documents \
  --operations search \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs generate \
  --skill usgs-water-iv-fetch \
  --capability usgs.water-instantaneous-values \
  --operations fetch \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs generate \
  --skill open-meteo-air-quality-fetch \
  --capability open-meteo.air-quality \
  --operations fetch-hourly \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs verify \
  --binding airnow-hourly-obs-fetch/references/tiangong-data-binding.json \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs verify \
  --binding federal-register-doc-fetch/references/tiangong-data-binding.json \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs verify \
  --binding usgs-water-iv-fetch/references/tiangong-data-binding.json \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs verify \
  --binding open-meteo-air-quality-fetch/references/tiangong-data-binding.json \
  --cli-version X.Y.Z
```

## 回退和删除纪律

- 计划 PR 不删除业务文件。
- Skill 实现 PR 只有在正式 CLI 版本可安装、binding 已验证且迁移 smoke 通过后才删除
  旧脚本。
- CLI connector 若发布后回退，Skills binding 同步回到仍受支持的正式 CLI；不能静默
  指向 branch 或本地 checkout。
- 不清理旧仓库、旧 worktree 或用户未提交内容；归档/删除需要另一次明确授权。

## 准备完成定义

准备完成是指：最新主分支、干净 worktree、两仓权威计划、候选清单、首批范围、TS7
依赖、PR 顺序、验证和回退门槛均已持久化并通过治理检查。准备完成后停止，等待下一次
实现授权。
