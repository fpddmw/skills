---
docType: runbook
scope: repo
status: proposed
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
lastReviewedCommit: f45607fcf63924c915d6a9a28f81686a694754b0
---

# 原子数据 Skill 迁移实施计划

## 当前基线和停止点

- 计划基线：`origin/main` at
  `4104e527facd09ecc242dad7a1e9645adf9d21f0`。
- 计划分支：`codex/atomic-data-plan`，使用独立干净 worktree。
- 原 Skills checkout、现有 `codex/atomic-environment-data-skills` worktree 及 CLI 的
  未提交变更都保持不动，不 stash/reset/rebase/clean。
- 本 runbook、架构和治理路由通过验证后停止；下一步需重新确认后才修改 Skill。

## 2026-08-30 实现状态

- CLI PR #71 已合并到 `tiangong-ai/cli` 主分支，merge commit 为 `832e302`；
  TypeScript 7、data runtime、AirNow、Federal Register 以及 Execution Manifest /
  Discovery Metadata 分层均已进入源码主线。
- 当前可安装的 `@tiangong-ai/cli@0.0.53` 尚不包含 `data` 命令，因此仍未达到删除
  Skill 旧执行脚本和提交正式 binding 的门槛。
- Skills 仓库已增加 execution-only binding 生成/校验器及离线 stale-binding 测试。
  本地从合并源码打出的候选包可生成并验证两个试点 binding，但候选包版本不得冒充
  正式发布版本写入 Skill。

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

候选：USGS Water IV、Open-Meteo 三个能力、NASA FIRMS、OpenAQ、Regulations.gov。

先用 USGS/Open-Meteo 扩展时间序列、空间范围和变量语义，再用 NASA FIRMS/OpenAQ/
Regulations.gov 验证真实 logical credential/provider-auth 诊断。每个 connector 单独批准，
不因共享 provider 品牌而把多个 operation 合成一个巨型 Skill。

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
node scripts/data-skill-binding.mjs verify \
  --binding airnow-hourly-obs-fetch/references/tiangong-data-binding.json \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs verify \
  --binding federal-register-doc-fetch/references/tiangong-data-binding.json \
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
