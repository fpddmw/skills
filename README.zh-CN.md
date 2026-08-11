---
docType: guide
scope: repo
status: current
authoritative: true
owner: skills
language: zh-CN
whenToUse: "安装、更新或使用 Tiangong AI 可复用 skills 仓库时。"
whenToUpdate: "当安装命令、目标 agent、安装范围、环境变量或 skill 可用性说明变化时。"
checkPaths:
  - AGENTS.md
  - .docpact/config.yaml
  - README.md
  - .claude-plugin/**
  - "*/SKILL.md"
lastReviewedAt: 2026-08-11
lastReviewedCommit: a5877c2b6520af97397e4ea6d82277a8de1de41a
---

# 天工 AI Skills

仓库地址: https://github.com/tiangong-ai/skills

请使用 https://github.com/vercel-labs/skills 提供的 `skills` CLI 来安装、更新和管理这些 skills。

## 安装 CLI
```bash
npm i skills -g
```

## 安装
- 仅列出可用技能（不安装）:
  ```bash
  npx skills add https://github.com/tiangong-ai/skills --list
  ```
- 安装全部技能（默认项目级）:
  ```bash
  npx skills add https://github.com/tiangong-ai/skills
  ```
- 安装指定技能:
  ```bash
  npx skills add https://github.com/tiangong-ai/skills --skill tiangong-auto-research --skill tiangong-kb-sci-search
  ```

## 目标 agent 与作用域
- 指定 agent:
  ```bash
  npx skills add https://github.com/tiangong-ai/skills -a codex -a claude-code
  ```
- 全局安装（用户级）:
  ```bash
  npx skills add https://github.com/tiangong-ai/skills -g
  ```
- 作用域说明:
  - Codex 是 universal agent：项目级使用 `./.agents/skills`，全局使用
    `$HOME/.agents/skills`；`CODEX_HOME` 不会改变 `skills@1.5.22` 的全局目标。
  - Claude Code 项目级使用 `./.claude/skills`；全局优先使用
    `$CLAUDE_CONFIG_DIR/skills`，否则使用 `$HOME/.claude/skills`。
  - 其他 agent 有各自目录；应查看精确锁定的 `skills` CLI 返回路径，不要按
    `~/<agent>/skills` 类推。

## 安装方式
- 交互式安装可选:
  - Symlink (recommended)
  - Copy

## 更新与确认
- 列出已安装技能:
  ```bash
  npx skills list
  ```
- 检查更新:
  ```bash
  npx skills check
  ```
- 更新全部技能:
  ```bash
  npx skills update
  ```

## 环境变量

环境变量要求由各 skill 自己维护。使用会调用外部服务的 skill 前，优先阅读该
skill 的 `references/env.md`（如存在）。

## Auto Research 外部 Skill 配置

普通 Skill 管理可以直接使用 `npx skills`。Auto Research workspace 应优先使用
CLI 的防呆 setup 层：底层仍调用精确锁定的 `skills` CLI，同时额外绑定来源
commit、Skill tree hash、目标目录、许可证选择、安全凭据绑定和审计状态。普通用户
可在 Wizard 中隐藏输入每个已选 Key；命名环境变量和有界 stdin/密码管理器输入仍是
显式可选方式。先查看只读目录，或启动交互式 Wizard：

```bash
REVIEWED_BOOTSTRAP_CLI_VERSION=X.Y.Z # 替换为已审阅的精确稳定版本
npx --yes --package "@tiangong-ai/cli@$REVIEWED_BOOTSTRAP_CLI_VERSION" -- \
  tiangong-ai research setup catalog \
  --workspace /absolute/path/to/workspace --json
npx --yes --package "@tiangong-ai/cli@$REVIEWED_BOOTSTRAP_CLI_VERSION" -- \
  tiangong-ai research setup \
  --workspace /absolute/path/to/workspace
```

bootstrap 版本是新 workspace 的显式选择，不得使用 `latest`、tag 或 range。
apply 创建 `runtime-lock.json` 后，已安装 orchestrator 的内置 resolver 会让
所有 workspace 操作只运行该锁定版本。

目录还提供 `tiangong-auto-research` 工作流 orchestrator、默认基线 Brave
互联网证据能力、可选的 Tiangong SCI/文档解析/论文获取 companion，以及可选的
Anthropic 或 PPT Master 闭环后创作 Skills。workspace 可以是用户指定的任意目录。
所有条目都是外生、独立授权、精确锁定且经用户明确确认或选择；研究 package
不会捆绑或安装它们。完整流程见
`tiangong-auto-research/references/setup.md` 和 `external-skills.md`。
创建 PPT 时首选 PPT Master；Anthropic PPTX 仍是兼容的按场景选项，需要时可在
同一显式计划中一起选择。
