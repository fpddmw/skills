---
docType: architecture
scope: repo
status: proposed
authoritative: true
owner: skills
language: zh-CN
whenToUse: "规划或修改原子数据 Skill、CLI 数据能力绑定或 Auto Research 数据能力入口时。"
whenToUpdate: "当仓库边界、薄 Skill 结构、兼容绑定、迁移范围或 CLI/Research 交接变化时。"
checkPaths:
  - AGENTS.md
  - .docpact/config.yaml
  - _docs/architecture/repo-architecture.md
  - _docs/contracts/repo-contract.md
  - _docs/runbooks/atomic-data-skill-migration.md
  - "*-fetch/**"
  - "*-search/**"
  - "*-download/**"
  - tiangong-auto-research/**
lastReviewedAt: 2026-08-30
lastReviewedCommit: 4104e527facd09ecc242dad7a1e9645adf9d21f0
---

# 原子数据 Skill 目标架构

## 决策

现有数据 fetch/search/download Skills 中可复用的 provider 业务逻辑将按评审结果重写到
Tiangong CLI 的 TypeScript 7.x 数据运行时。Skills 仓库继续拥有面向 agent 的语义
入口，但不再维护第二份 Python/JavaScript connector、HTTP/认证/分页/重试实现或闭合
机器 Schema。

旧 `openclaw-eco-concil_v1` 及现有 Skill 脚本只作为只读迁移输入，用于核对来源知识、
外部行为、字段、限制和 privacy-safe fixtures。迁移不合并旧 Git 历史，不保留 Python
兼容层，也不迁入 OpenClaw harness、议会/多 agent 编排、跨 round/case 数据库或案例
工作流。

CLI 仓库中的 `docs/agents/data-runtime-architecture.md` 是命令、manifest、Schema、错误、
回执、凭证和执行行为的权威目标契约；本文件只规定 Skills 如何消费它。

## 所有权

| 内容 | 所有者 | Skills 侧规则 |
| --- | --- | --- |
| 触发条件、使用场景、参数解释 | Skills | 写入 `SKILL.md`，为 agent 提供语义入口 |
| 来源说明、许可、限制、引用建议 | Skills | 保留精炼 references，不复制 provider 实现 |
| capability/operation/CLI 兼容绑定 | Skills | 保存最小机器可检验 binding |
| connector、Schema、错误码、回执 | CLI | Skills 只验证 digest，不复制定义 |
| HTTP、认证、分页、限流、缓存 | CLI | Skill 不再直接执行网络业务代码 |
| 多源选择、证据准入、研究持久化 | Auto Research | 不下沉到原子 Skill |
| 旧 Python/OpenClaw 实现 | 只读迁移输入 | 正式路径不得依赖 |

CLI 是先确认、先实现、先发布的基座。Skills 计划可以与 CLI 计划同步评审，但生产 Skill
不能先合并一份指向尚不存在命令、未冻结 Schema 或未发布版本的绑定。

## 标准薄 Skill

迁移后的原子数据 Skill 预期只包含：

```text
<skill>/
├── SKILL.md
├── agents/openai.yaml
└── references/
    ├── tiangong-data-binding.json
    ├── source-notes.md
    └── limitations.md
```

具体 Skill 可在确有需要时保留其他非执行型 reference/asset，但默认移除：

- provider fetch Python/JavaScript 脚本和依赖；
- `config.example.env` 中由 CLI 负责的运行时配置；
- OpenClaw/eco-council chaining 模板和 raw artifact 路径约定；
- 重复的输入/输出字段表、重试算法、HTTP 参数构造和 provider 响应校验代码。

`SKILL.md` 应说明何时调用、需要哪些非敏感参数、来源和限制、如何用文件/stdin 调用
精确 CLI operation，以及结果不能支持哪些推断。凭证只以逻辑需求呈现，真实值由 CLI
解析；示例不得把 secret 放进命令行或 JSON。

## 最小兼容绑定

每个迁移后的 Skill 保存一份由 CLI canonical catalog 导出的
`references/tiangong-data-binding.json`，至少绑定：

- binding schema 版本；
- `capabilityId` 与 `capabilityVersion`；
- `minimumCliVersion`；
- 每个允许 operation 的 `operationId`、版本、输入/输出 Schema digest；
- manifest digest 和生成该绑定的精确 CLI 版本。

该文件不是第二份 manifest 或 Schema。Skills CI 使用对应 CLI 包重新导出摘要并比较，
拒绝 capability 缺失、版本过低、operation 漂移或 digest 不一致。`SKILL.md` 和生成的
agent metadata 不再散落多份 CLI 版本常量。

## 调用边界

薄 Skill 通过 proposed 公共命令族调用一次原子 operation：

```text
tiangong-ai data catalog
tiangong-ai data describe <capability-id>
tiangong-ai data doctor <capability-id> [--live]
tiangong-ai data run <capability-id> <operation-id> --input <path|->
```

这些命令在 CLI 基础契约合并前仍为 proposed，不能提前写成用户可用事实。Skill 不
自行发现其他来源、不跨来源 fan-out、不解释研究结论。一个来源内部有界的分页或多
文件窗口仍可以是一个 operation；跨来源组合必须由 Auto Research 或显式上层调用者
完成。

Auto Research 接入同一 CLI 内部数据服务后，原子 Skill 与 Research 应共享相同核心
结果/回执。Research 另外负责 capability lock、预算、来源/证据准入、永久 evidence、
journal、handoff 和 review；这些状态不得回流到薄 Skill。

## 原子性与独立性

不要求每个能力有独立进程或 npm 包，但要求：

- 每个 Skill 只绑定一个清晰 provider/capability 语义；
- 每个 operation 可单独 catalog、describe、doctor、run 和测试；
- 一个 connector 的凭证、失败、缓存或 partial 结果不污染其他能力；
- Skill 不调用另一个 Skill 来完成自己的核心 fetch；
- 通用 contract 只统一运行和 provenance，不把异构来源强压成巨型业务 Schema。

## 迁移分类

目录名包含 fetch/search/download 不等于都应迁入通用 data runtime。所有候选先归入：

1. **原子 provider connector**：有稳定官方 endpoint、闭合参数/结果和明确许可，适合
   CLI `data`。
2. **内容获取/媒体工作流**：涉及正文解析、浏览器、下载产物或平台特定语义，需要单独
   判断是否仍是 connector。
3. **Tiangong/KB/Research 产品能力**：已有明确产品命令或研究边界，不因名称含
   `search`/`fetch` 而迁移。
4. **私有账户/本地输入能力**：例如 IMAP，需独立安全评审，不自动进入公共数据面。
5. **退役/合并**：价值、许可、API 稳定性或维护成本不足，不追求旧能力数量对等。

具体目录清单、批次和完成门槛见
`_docs/runbooks/atomic-data-skill-migration.md`。

## TypeScript 7 边界

用户要求的 TypeScript 7 重写发生在 CLI 仓库。Skills 侧不再保留数据业务实现，因此
不为 connector 新增另一套 TypeScript 脚本。CLI 的 TypeScript 7 工具链升级先作为
独立门槛通过，随后才实现公共合同和 connector；Skill 迁移等候已发布的兼容 CLI。

## 完成定义

一次原子 Skill 迁移完成必须同时满足：

- CLI 中已有已发布、TypeScript 7 实现的 capability/operation 及闭合测试；
- Skill 只保留语义入口、来源/限制和可检验 binding；
- 无 Python/旧 harness/OpenClaw 执行依赖，无重复机器 Schema；
- 离线 stale-binding、skill-creator、安装 smoke 和 docpact 门禁通过；
- Research 如需该来源，通过 CLI adapter 复用核心结果，而不是继续执行 Skill 脚本。

本次计划持久化不授权立即删除任何现有脚本或修改 Skill。删除只发生在对应 CLI 能力
已验收并有明确迁移/回退路径的实现 PR 中。
