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
lastReviewedAt: 2026-08-31
lastReviewedCommit: 09d49fcce0871ac97997c4e5e79975ae29c79c84
---

# 原子数据 Skill 迁移实施计划

## 当前基线

- 计划基线：`origin/main` at
  `4104e527facd09ecc242dad7a1e9645adf9d21f0`。
- 计划分支：`codex/atomic-data-plan`，使用独立干净 worktree。
- 原 Skills checkout、现有 `codex/atomic-environment-data-skills` worktree 及 CLI 的
  未提交变更都保持不动，不 stash/reset/rebase/clean。
- 首批实现继续使用该独立 worktree；正式 CLI 包、binding 和全部门禁通过后才提交 PR。

## 2026-08-31 实现状态

- CLI PR #71 已合并到 `tiangong-ai/cli` 主分支，merge commit 为 `832e302`；
  TypeScript 7、data runtime、AirNow、Federal Register 以及 Execution Manifest /
  Discovery Metadata 分层均已进入源码主线。
- 当前公共 `@tiangong-ai/cli@0.0.54` 尚不包含 `data` 命令，因此仍未达到删除
  Skill 旧执行脚本和提交正式 binding 的门槛。
- Skills 仓库已增加 execution-only binding 生成/校验器及离线 stale-binding 测试。
  AirNow、Federal Register、USGS Water IV、Open-Meteo Air Quality、Open-Meteo Flood、
  Open-Meteo Historical Weather、NASA FIRMS、OpenAQ、Regulations.gov Comments、
  Regulations.gov Comment Details，以及 GDELT DOC、Events、GKG、Mentions 已在本地
  候选分支薄化；Bluesky Cascades、YouTube Video Search 与 YouTube Comments 也已在
  完成逐项语义/许可/安全审计后薄化。两个 Regulations.gov Skill 分别绑定同一
  `regulations-gov.comments` capability 的 search 与 fetch-details operation；四个
  GDELT Skill 分别绑定独立 capability；两个 YouTube Skill 分别绑定同一
  `youtube.public-content` capability 的 search-videos 与 fetch-comments operation。
  十七项旧 Python connector 与重复 provider references 已移出候选 Skill，并共同纳入
  copy/symlink 安装 smoke。
- 当前本地候选包 `0.0.55` 只用于分支内兼容验证，不代表 npm 正式发布。PR 前必须用
  实际包含全部十五个 capability 的正式版本重新生成十七个 binding，并用该 npm 包
  重跑全部门禁。

## 2026-08-31 本地完成性审计

- 仓库中实际存在 38 个 fetch/search/download 候选：17 个原子 provider Skill 已薄化，
  其余 21 个均落入下文记录的内容/文件、产品或私有账户保留边界；没有未分类目录。
- CLI 候选使用 Node 24、TypeScript 7.0.2 和版本 `0.0.55`，发布 15 个 capability；
  17 个薄 Skill 的 `generatedWithCliVersion` 与 `minimumCliVersion` 均为 `0.0.55`。
- 每个薄 Skill 只含 `SKILL.md`、`agents/openai.yaml` 和
  `references/tiangong-data-binding.json`；原 Python connector、provider 配置、重复 API
  notes 和 OpenClaw 模板均不在生产 Skill 路径中。
- 17 个 binding 已逐项对照最终候选的 execution manifest 与 operation 输入/输出 Schema
  digest；copy/symlink 隔离安装 smoke 使用最终 tarball 通过，且不访问真实 provider。
- CLI 的 lint、typecheck、486 项全量测试、3 项 platform contract、coverage、npm pack、
  immutable setup pin audit 和 docpact 均通过；17 个薄 Skill 与 21 个明确保留 Skill 的
  `quick_validate.py` 均通过，binding contract 与 docpact 也通过。
- 两个仓库的 cold-container 命令已实际执行，但当前主机的 Docker Desktop 服务及
  `docker-desktop` WSL 后端未运行，均在构建前以 `docker` 不可用退出；这是唯一未通过的
  PR 前门禁。Docker 环境恢复后必须重跑 CLI `npm run test:clean:cold` 与 Skills
  `scripts/test-clean-container.sh --cold-build`，不能用宿主测试替代。
- CLI Research adapter/必要的 Auto Research 变更仍是下文明确分离的后续工作包，不被
  伪装为本次 17 个原子 Skill 迁移的完成证据。
- 当前只有本地分支提交；在维护者统一审阅并明确确认前，不推送实现分支、不创建 PR。

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
- `bluesky-cascade-fetch`
- `youtube-video-search`
- `youtube-comments-fetch`

### B. 审计后保留专用内容/媒体/下载边界

- `ai-tech-rss-fetch`
- `ai-tech-fulltext-fetch`
- `eceee-news-fulltext-fetch`
- `sustainability-rss-fetch`
- `sustainability-fulltext-fetch`
- `figshare-data-download`
- `academic-paper-download`

RSS Skill 的核心包括任意 feed/OPML intake、SQLite subscription state、dedupe 与
incremental sync；fulltext Skill 的核心包括 HTML/body acquisition、正文解析、retry queue
与持久化。Figshare 的交付物是浏览器获取的本地文件 artifact；论文下载是带合法开放获取
路径、浏览器 handoff、PDF/hash/manifest/provenance 的 Research companion。把这些行为
替换成无状态 JSON connector 会实质丢失功能并引入不同的动态 URL、内容安全、文件与状态
合同，因此它们不是待办迁移，而是经审计后继续保持现有专用边界。

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

| Skill                          | 保留到 CLI capability 的核心行为                                                                                                                                                       | 有意收敛或替代的旧行为                                                                                                                                                                                                                                                                                                                                     |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| AirNow                         | UTC 小时文件规划、bbox/time/pollutant 过滤、站点小时记录、逐文件 lineage、缺失/坏文件 partial                                                                                          | 任意 endpoint/path/user-agent/重试调参、Skill `check-config`、dry-run、日志文件和 raw artifact 写入被 CLI manifest、`data doctor`、统一 limits、run-result/receipt 取代；非整点输入不再静默截断而是拒绝                                                                                                                                                    |
| Federal Register               | publication date、term、agency、type、topic、docket、RIN、稳定查询编码、有界分页/记录、空结果/截断/later-page partial                                                                  | `section`、`significant` 输入过滤、任意 `fields` 投影和 `executive_order_number` 排序不属于首个 capability v1；Skill 不再宣称支持。每次调用的 page/record 上限改用 run-request 顶层 `limits` 且只能降低 manifest 上限；dry-run、日志和 raw artifact 路径不再保留                                                                                           |
| USGS Water IV                  | bbox 或最多 100 个 sites、period 或显式 window、参数/site type/status/agency 过滤、WaterML series/value 归一化、qualifier/provisional、no-data 过滤和坏 row/series partial             | 旧脚本允许本地 env/argv 覆盖 endpoint、重试、节流、上限、user-agent、日志和 `file://` fixture，并提供 `check-config`、dry-run、raw artifact 写入；这些改由 CLI endpoint policy、manifest limits、static doctor、fixture tests 和 run-result/receipt 取代。官方 legacy 上限把旧 Skill 的 200 sites 收紧为 100，且明确 2027-Q1 decommission 风险             |
| Open-Meteo Air Quality         | 最多 10 个坐标、92 个闭合日期、16 个官方 hourly variables、domain/cell selection、单次多坐标响应、nullable aligned arrays 和坐标/变量 partial                                          | timezone 固定为 GMT，删除任意 timezone、endpoint、API key、重试、节流、user-agent、日志、dry-run 和 raw artifact 调参。公开 endpoint 明确为 non-commercial 且无凭证；商业 customer endpoint/API key 需要独立 capability 评审。输出改为 location-hour 列式结果和统一 run-result/receipt，不兼容旧 snake_case payload                                        |
| Open-Meteo Flood               | 最多 10 个坐标、366 个闭合日期、7 个官方 daily discharge variables、cell selection、optional ensemble members、nullable aligned arrays 和坐标/变量/member partial                      | timezone 固定为 GMT，ensemble 必须同时请求 `river_discharge`；删除任意 timezone、endpoint、API key、重试、节流、user-agent、日志、dry-run 和 raw artifact 调参。公开 endpoint 明确为 non-commercial 且无凭证。输出按 location-day 计数并显式区分 requested/river-grid coordinate，不把 GloFAS simulated discharge 误写成 gauge observation、告警或严重度   |
| Open-Meteo Historical Weather  | 最多 10 个坐标、366 个闭合日期、一个受控 model、12 个 curated hourly 与 12 个 curated numeric daily variables、多坐标响应、nullable aligned arrays 和坐标/section/变量 partial         | timezone 与单位固定为 GMT/摄氏度/km/h/mm；两个变量数组显式传入且至少一方非空。删除任意 timezone、endpoint、API key、任意 model、重试、节流、user-agent、日志、dry-run 和 raw artifact 调参。公开 endpoint 明确为 non-commercial 且无凭证；输出按 location 内 hourly 后 daily 的时间行计数，明确 reanalysis/model grid 并提示长期趋势使用 ERA5 或 ERA5-Land |
| NASA FIRMS Active Fire         | 一个 reviewed source、非跨日界线 bbox、最多 31 个闭合 UTC 日期、可选 availability probe、五天分片、transaction/record cap、公共 MODIS/VIIRS/Landsat detection 字段和 chunk/row partial | `NASA_FIRMS_MAP_KEY` 只由 CLI 从进程环境解析并作为受保护 path segment 注入；删除 Skill-local env 文件、endpoint/retry/throttle/user-agent/log/dry-run/raw artifact 调参和 OpenClaw fan-out。输出改为统一 run-result/receipt，不把 thermal anomaly 误写成 fire perimeter、burned area、incident、cause 或 alert                                             |
| OpenAQ Air Quality             | 有界 location discovery、provider/owner/sensor/parameter/license/coverage metadata，以及单 sensor、最长 366 天的 raw/hourly/daily measurements、稳定分页和 later-page partial          | 任意 v3 path/query、API/S3 自动路由、S3 prefix/list/download、Skill-local region/bucket/endpoint 配置和 Python client/router 被移除；`OPENAQ_API_KEY` 只由 CLI header 注入。批量 archive 文件转入单独 content/download 审计；输出不提供 AQI、健康/监管判断、跨 sensor 聚合、单位转换或来源归因                                                             |
| Regulations.gov Comments       | posted 或 last-modified 二选一的最长 366 天窗口、agency/comment-on/search-term 收窄、稳定 JSON:API 分页、comment ID/日期/标题/withdrawal metadata 和 later-page partial                | `REGGOV_API_KEY` 只由 CLI header 注入；移除任意 endpoint、重试/节流/log/dry-run、JSONL 写入和 quarantine。结果明确不是代表性公众意见、投票或统计 sentiment，不提供 comment post/modify、detail body 或 attachment download                                                                                                                                 |
| Regulations.gov Comment Detail | 最多 100 个显式 comment ID、caller 顺序、comment/docket/document linkage、日期、withdrawal/restriction、组织上下文、duplicate count、可选 attachment metadata 和 per-ID partial        | 删除本地文件 ID 解析、任意 endpoint、重试/节流/log/dry-run、raw/JSONL/quarantine 写入；CLI 以 allowlist 排除姓名、邮箱、电话、地址与 locality 等个人 profile 字段，只返回 attachment metadata/link，不下载 bytes，也不提供法律判断或代表性 sentiment                                                                                                       |
| GDELT DOC                      | 有界 relative/absolute window、受控 article-list/timeline modes、GDELT query syntax、模式化 JSON 结果和 provider truncation/空结果语义                                               | 删除任意 DOC mode/format/额外 query 参数、endpoint/retry/throttle/log 配置和 raw artifact 写入；只返回文章 metadata/link 或聚合时间线，不下载正文，也不把自动 tone/count/ranking 解释为代表性、事实或因果证据                                                                                                                                                |
| GDELT Events                   | latest 或精确 15 分钟 UTC range、最多 20 个 source files、ZIP/MD5/CRC/UTF-8/61-column 校验、机器编码 event rows 与来源 lineage                                                        | 删除 masterfilelist 下载、dry-run、任意 expected-columns、output/quarantine/log 路径和持久 ZIP；CLI 返回有界内存归一化 rows 与统一 receipt，不把 coded event 当作验证事实、唯一事件或正文                                                                                                                                                                      |
| GDELT GKG                      | latest 或精确 15 分钟 UTC range、最多 20 个 source files、ZIP/MD5/CRC/UTF-8/27-column 校验、GKG annotations 与 document lineage                                                       | 删除 masterfilelist 下载、dry-run、任意 expected-columns、output/quarantine/log 路径和持久 ZIP；CLI 返回有界内存归一化 rows，不把 machine-extracted themes/entities/locations/tone 当作已验证知识、正文或 sentiment ground truth                                                                                                                              |
| GDELT Mentions                 | latest 或精确 15 分钟 UTC range、最多 20 个 source files、ZIP/MD5/CRC/UTF-8/16-column 校验、mention-level provenance/confidence/source linkage                                       | 删除 masterfilelist 下载、dry-run、任意 expected-columns、output/quarantine/log 路径和持久 ZIP；CLI 返回有界内存归一化 rows，不把 mention 当作独立 endorsement、唯一文章、验证事件或正文                                                                                                                                                                    |
| Bluesky Cascades               | public search/author/custom/list seed source、optional UTC window、seed post normalization、visible `getPostThread` reply topology、blocked/not-found node 与 per-thread partial                               | 删除 optional auth/base URL fallback、Skill-local env、retry/throttle/log/dry-run、JSON/JSONL artifact 和 OpenClaw 配置；CLI 统一 bounded HTTP/receipt，只开放 public AppView，把 ranking/feed/indexing/moderation/counters 明确为可变快照，不宣称 archive completeness、代表性、事实、身份、sentiment 或 causal diffusion evidence                              |
| YouTube Video Search           | query/channel/published/order/region/language/safe-search、十种 video filter、search pagination、`videos.list` detail enrichment、public comment/view threshold                                  | `YOUTUBE_API_KEY` 只经 CLI `X-Goog-Api-Key` header 注入；删除 query-string key、endpoint/retry/throttle/log/dry-run、JSONL/quarantine/artifact 输出。CLI 始终执行 detail enrichment，不下载 media/caption/transcript/thumbnail，不把 search ranking 或统计当作代表性、endorsement、truth 或 sentiment                                                                  |
| YouTube Comments               | 显式 video IDs、optional UTC window 与 published/updated 选择、thread order/search terms、top-level comments、`comments.list` 完整可见 replies 分页、per-video partial                         | 删除本地 txt/json/jsonl ID 文件解析、HTML text mode、endpoint/retry/throttle/log/dry-run、JSONL/quarantine/artifact 输出；plainText 固定，API key 只由 CLI header 注入。operation-wide 与 per-video/per-thread limits 取代多套脚本 cap；评论不被解释为代表性 opinion、身份、事实、人口属性或 sentiment ground truth                                                        |

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

USGS Water IV、Open-Meteo Air Quality、Open-Meteo Flood、Open-Meteo Historical
Weather、NASA FIRMS、OpenAQ，以及 Regulations.gov comments/detail 两个语义入口已在
本地完成 CLI connector 与 Skill 薄化。Regulations.gov 两个 Skill 共享一个 capability，
但保持独立意图入口和单 operation binding。

USGS 已扩展时间序列、空间范围、变量、qualifier 和 legacy 生命周期语义；Open-Meteo
Air Quality 已验证模型网格、GMT 列式多变量数据、public/commercial endpoint 分离和
attribution 语义；Flood 已验证 GloFAS river-grid、forecast-only statistics、ensemble
members 与非 gauge/alert 边界；Historical 已验证受控单模型、hourly/daily 双粒度、
reanalysis 与 station observation 区分，以及跨年代模型一致性提示；NASA FIRMS 已验证
path-segment logical credential、quota estimate、CSV chunk 与 hotspot/non-perimeter 边界；
OpenAQ 已验证 header credential、location discovery、单 sensor raw/hourly/daily、双
operation binding、source-specific attribution 和 S3 download 分层；Regulations.gov
已验证 provider-auth、JSON:API pagination、Eastern wall-clock filter、个人字段 allowlist、
attachment metadata 与 per-ID partial。每个 connector 单独批准，不因共享 provider
品牌而把多个 operation 合成一个巨型 Skill。

### 批次 3：GDELT 与内容/社交来源

GDELT 已决定保持四个独立 capability：DOC 搜索的 API/模式化聚合语义与三个文件 feed
不同；Events、GKG、Mentions 共享 CLI 内部有界 ZIP/TSV 核心，但分别拥有闭合输出 Schema
和独立发现语义。四个 Skill 已在本地候选分支完成薄化、execution-only binding 和统一
安装 smoke 接入。

Bluesky 与 YouTube 的审计结论是：它们的公开、闭合、只读 API operation 适合 data
runtime。CLI 已完成 `bluesky.public-posts/fetch-cascades` 以及
`youtube.public-content/search-videos|fetch-comments`；三个 Skill 已薄化、绑定同一精确本地
候选包并进入统一安装 smoke。YouTube key 仅经 `X-Goog-Api-Key` header 注入；comments
operation 使用 `comments.list` 展开 replies，不信任 embedded reply sample。

RSS/fulltext、Figshare 与 academic paper 的审计结论相反：它们分别拥有持久订阅/正文队列、
浏览器文件 artifact 或 Research acquisition/provenance 核心，因此继续保持现有专用实现，
不是未完成的原子迁移。

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
结构化阻断请求，不访问 AirNow、Bluesky、FederalRegister.gov、GDELT、NASA FIRMS、
OpenAQ、Open-Meteo、Regulations.gov、USGS WaterServices 或 YouTube。

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
node scripts/data-skill-binding.mjs generate \
  --skill open-meteo-flood-fetch \
  --capability open-meteo.flood \
  --operations fetch-daily \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs generate \
  --skill open-meteo-historical-fetch \
  --capability open-meteo.historical-weather \
  --operations fetch \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs generate \
  --skill nasa-firms-fire-fetch \
  --capability nasa-firms.active-fire \
  --operations fetch-area \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs generate \
  --skill openaq-data-fetch \
  --capability openaq.air-quality \
  --operations search-locations,fetch-sensor-measurements \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs generate \
  --skill regulationsgov-comments-fetch \
  --capability regulations-gov.comments \
  --operations search \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs generate \
  --skill regulationsgov-comment-detail-fetch \
  --capability regulations-gov.comments \
  --operations fetch-details \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs generate \
  --skill gdelt-doc-search \
  --capability gdelt.doc-search \
  --operations search \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs generate \
  --skill gdelt-events-fetch \
  --capability gdelt.events \
  --operations fetch \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs generate \
  --skill gdelt-gkg-fetch \
  --capability gdelt.gkg \
  --operations fetch \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs generate \
  --skill gdelt-mentions-fetch \
  --capability gdelt.mentions \
  --operations fetch \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs generate \
  --skill bluesky-cascade-fetch \
  --capability bluesky.public-posts \
  --operations fetch-cascades \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs generate \
  --skill youtube-video-search \
  --capability youtube.public-content \
  --operations search-videos \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs generate \
  --skill youtube-comments-fetch \
  --capability youtube.public-content \
  --operations fetch-comments \
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
node scripts/data-skill-binding.mjs verify \
  --binding open-meteo-flood-fetch/references/tiangong-data-binding.json \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs verify \
  --binding open-meteo-historical-fetch/references/tiangong-data-binding.json \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs verify \
  --binding nasa-firms-fire-fetch/references/tiangong-data-binding.json \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs verify \
  --binding openaq-data-fetch/references/tiangong-data-binding.json \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs verify \
  --binding regulationsgov-comments-fetch/references/tiangong-data-binding.json \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs verify \
  --binding regulationsgov-comment-detail-fetch/references/tiangong-data-binding.json \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs verify \
  --binding gdelt-doc-search/references/tiangong-data-binding.json \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs verify \
  --binding gdelt-events-fetch/references/tiangong-data-binding.json \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs verify \
  --binding gdelt-gkg-fetch/references/tiangong-data-binding.json \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs verify \
  --binding gdelt-mentions-fetch/references/tiangong-data-binding.json \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs verify \
  --binding bluesky-cascade-fetch/references/tiangong-data-binding.json \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs verify \
  --binding youtube-video-search/references/tiangong-data-binding.json \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs verify \
  --binding youtube-comments-fetch/references/tiangong-data-binding.json \
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
