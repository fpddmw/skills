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
lastReviewedAt: 2026-08-06
lastReviewedCommit: 0d660c7401e5e307f742571dda7854500b38d9af
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
  - 项目级安装到 `./<agent>/skills/`.
  - 全局安装到 `~/<agent>/skills/`.

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

## Auto Research 外部证据能力

`tiangong-auto-research` 负责协调外生的证据 Skills；本仓库本身不是生产研究的
证据提供者目录。在目标 workspace 中，用以下命令查看精确锁定的推荐项、安装
计划、服务商要求和当前状态：

```bash
npx --yes @tiangong-ai/cli@0.0.23 research capability catalog \
  --path /absolute/path/to/workspace --json
```

默认 `internet-research` profile 需要外部 `web-search`、`news-search` Skills，
以及用户持有的 Brave Search API key。增强上下文和媒体 profile 必须显式选择，
并受服务商套餐约束。用户有权访问的数据库通过外部、不可变的 Skill 定义逐个
导入。完整流程见 `tiangong-auto-research/references/external-skills.md`；研究
runtime 不会自动安装这些依赖。
