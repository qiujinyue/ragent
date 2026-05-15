# AGENTS.md

This file provides behavioral guidelines for AI agents working in the Ragent codebase.

## 开发规范

- 使用 `./mvnw` (后端) 和 `npm run` (前端) 执行构建命令，不要直接调用 maven/npm
- 代码提交前先运行 `./mvnw spotless:apply` 格式化
- 前端提交前运行 `npm run lint` 和 `npm run format`
- 测试：后端用 `./mvnw test`，前端用 `npm run test`

## 架构决策

- 遵循 CLAUDE.md 中定义的设计模式（Strategy、Factory、Registry、Template Method 等）
- 修改多层依赖时，遵循 `framework` → `infra-ai` → `bootstrap` 的依赖方向，禁止反向依赖
- 涉及架构重构时，先使用 Plan mode 规划并获得批准
- 8 个线程池的配置不可随意修改，详见 `ThreadPoolExecutorConfig.java`

## 安全约束

- 禁止执行破坏性命令：`rm -rf`（除非明确要求）、`git reset --hard`、强制推送
- 禁止提交敏感信息：API 密钥、数据库密码、私钥等
- 禁止跳过 pre-commit hooks
- 所有外部工具调用需明确用途

## 工作流程

- **创意/设计** → 使用 `superpowers:brainstorming` 技能
- **功能实现** → 使用 `superpowers:test-driven-development` 技能
- **代码审查** → 使用 `superpowers:requesting-code-review` 技能
- **完成验证** → 使用 `superpowers:verification-before-completion` 技能
- **多步骤任务** → 使用 Plan mode 规划
- 每次只做一个主题的修改，避免批量提交不相关变更
