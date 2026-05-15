# Ragent 项目深入分析

## 1. 项目结构

### Maven 模块

| 模块 | Artifact ID | 职责 |
|------|-------------|------|
| `bootstrap` | `bootstrap` | 主应用入口、Web控制器、核心业务逻辑、RAG pipeline、知识库管理、admin 仪表盘 |
| `framework` | `framework` | 共享基础设施：Redis、MyBatis-Plus、RocketMQ、Sa-Token 认证、分布式ID、幂等、AOP切面、链路追踪 |
| `infra-ai` | `infra-ai` | AI模型集成层：LLM客户端（Ollama/BaiLian/SiliconFlow）、Embedding、Rerank、模型路由与熔断 |
| `mcp-server` | `mcp-server` | 独立MCP协议服务器，JSON-RPC端点，工具注册与执行（Weather/Ticket/Sales） |

### 目录结构

```
ragent/
├── bootstrap/src/main/java/com/nageoffer/ai/ragent/
│   ├── admin/                    # 管理后台 API
│   ├── core/chunk/               # 文档分块策略（FixedSizeTextChunker, StructureAwareTextChunker）
│   ├── core/parser/              # 文档解析（TikaDocumentParser，支持 PDF/DOC/Markdown）
│   ├── ingestion/                # ETL 摄入流水线
│   ├── knowledge/                # 知识库管理（KB/文档/Chunk）
│   ├── rag/                      # RAG 核心
│   │   ├── config/               # 线程池配置（8个专用线程池）
│   │   ├── core/                  # 核心组件：intent/memory/retrieve/mcp/prompt
│   │   │   ├── intent/            # 意图树、意图分类器、意图解析器
│   │   │   ├── memory/            # 会话记忆服务（滑动窗口+摘要）
│   │   │   ├── retrieve/          # 多通道检索引擎（向量+意图导向）
│   │   │   ├── mcp/               # MCP工具注册表、HTTP客户端、参数提取
│   │   │   └── prompt/            # RAG提示词服务
│   │   └── service/               # RAG 服务实现
│   │       └── pipeline/          # StreamChatPipeline 核心流水线
│   │       └── streaming/         # 流式响应处理（SseEmitterSender, StreamCallbackFactory）
│   ├── user/                     # 用户管理
│   └── Main.java                 # 启动类
├── framework/src/main/java/com/nageoffer/ai/ragent/framework/
│   ├── cache/                    # Redis Key 序列化
│   ├── config/                   # 自动配置（DB、RocketMQ、Web）
│   ├── context/                  # 用户上下文、Trace上下文（TTL传播）
│   ├── convention/                # Result<T>、ChatRequest、RetrievedChunk 等约定对象
│   ├── database/                 # MyBatis-Plus 自动填充（Snowflake ID）
│   ├── distributedid/            # 自定义雪花ID算法
│   ├── errorcode/                # 三层异常体系（Client → Service → Remote）
│   ├── idempotent/               # 双重维度幂等（submit + consume）
│   ├── mq/                        # RocketMQ生产者适配器，支持事务
│   ├── trace/                    # @RagTraceNode AOP 链路追踪
│   └── web/                      # 全局异常处理器、SseEmitterSender（线程安全SSE）
├── infra-ai/src/main/java/com/nageoffer/ai/ragent/infra/
│   ├── chat/                     # ChatClient 接口及实现（BaiLian/Ollama/SiliconFlow）
│   ├── embedding/                 # EmbeddingClient 接口及实现（Ollama/SiliconFlow）
│   ├── rerank/                    # RerankClient 接口及实现
│   ├── model/                     # ModelCaller、ModelHealthStore（三态熔断器）
│   ├── token/                     # HeuristicTokenCounterService
│   └── http/                      # HttpResponseHelper、ModelClientException
├── mcp-server/src/main/java/com/nageoffer/ai/ragent/mcp/
│   ├── core/                      # MCPToolExecutor、MCPToolRegistry（注册表模式）
│   ├── endpoint/                  # MCPDispatcher、MCPEndpoint（JSON-RPC）
│   ├── executor/                  # Weather/Ticket/Sales MCP执行器
│   └── protocol/                  # JsonRpcRequest/Response
└── frontend/
    └── src/
        ├── components/           # UI组件（chat/admin/common/layout/session/ui）
        ├── hooks/                 # useStreamResponse.ts（SSE流式处理）
        ├── pages/                 # 页面：LoginPage、ChatPage、admin/*（知识库/意图树/摄入流水线/链路追踪等）
        ├── services/              # API客户端（axios）：authService、chatService、knowledgeService 等
        ├── stores/                 # Zustand状态管理：authStore、chatStore
        ├── types/                  # TypeScript 类型定义
        └── router.tsx              # React Router v6 配置（含权限守卫）
```

---

## 2. 核心架构设计

### 2.1 RAG Pipeline 流水线

**入口：** `StreamChatPipeline.execute()`

```
用户问题
    │
    ▼
┌─────────────────────────────┐
│ 1. loadMemory()              │ ← 加载会话历史，追加用户消息
└─────────────────────────────┘
    │
    ▼
┌─────────────────────────────┐
│ 2. rewriteQuery()            │ ← LLM 改写问题（子问题分解）
└─────────────────────────────┘
    │
    ▼
┌─────────────────────────────┐
│ 3. resolveIntents()          │ ← IntentResolver 分类子问题到意图树节点
└─────────────────────────────┘
    │
    ▼
┌─────────────────────────────┐
│ 4. handleGuidance()?         │ ← 检测模糊意图，主动询问确认（可短路）
└─────────────────────────────┘
    │
    ▼
┌─────────────────────────────┐
│ 5. handleSystemOnly()?       │ ← 纯系统意图（如问候），直接响应不检索
└─────────────────────────────┘
    │
    ▼
┌─────────────────────────────┐
│ 6. retrieve()                │ ← 并行：向量检索 + MCP工具调用
└─────────────────────────────┘
    │
    ▼
┌─────────────────────────────┐
│ 7. handleEmptyRetrieval()?   │ ← 无检索结果，返回提示
└─────────────────────────────┘
    │
    ▼
┌─────────────────────────────┐
│ 8. streamRagResponse()        │ ← 组装RAG prompt，流式返回
└─────────────────────────────┘
```

### 2.2 关键设计模式

| 模式 | 应用场景 |
|------|----------|
| **Pipeline** | `StreamChatPipeline` 串联 RAG 各阶段，每阶段返回 boolean 控制短路 |
| **Strategy** | `ChunkingStrategyFactory`（固定大小/结构感知分块）、`EmbeddingClient`、`ChatClient` |
| **Registry** | `MCPToolRegistry` 自动发现并注册所有 `MCPToolExecutor` Bean（`@PostConstruct`） |
| **Factory** | `IntentTreeFactory` 创建意图树、`StreamCallbackFactory` 创建流回调 |
| **Template Method** | `AbstractOpenAIStyleChatClient` 提供 SSE 解析和 HTTP 处理骨架 |
| **Decorator** | `ProbeBufferingCallback` 装饰模型探测回调 |
| **Chain of Responsibility** | `PostProcessor` 链（去重 → Rerank） |
| **Observer** | `StreamCallback` 流式回调观察者 |
| **AOP** | `@ChatRateLimit` 限流、`@IdempotentSubmit/@Consume` 幂等、`@RagTraceNode` 链路追踪 |

### 2.3 线程池架构

`bootstrap/src/main/java/com/nageoffer/ai/ragent/rag/config/ThreadPoolExecutorConfig.java`

8 个专用线程池，全部用 `TtlExecutors` 包装以支持 TransmittableThreadLocal 上下文传播：

| 线程池 | 用途 |
|--------|------|
| `mcp-batch` | MCP 批量工具调用 |
| `rag-context-assembly` | RAG 上下文组装 |
| `multi-retrieval` | 多通道检索并行执行 |
| `internal-retrieval` | 内部检索任务 |
| `intent-classify` | 意图分类 |
| `memory-summary` | 记忆摘要生成 |
| `model-stream` | 模型流式输出 |
| `chat-entry` | 聊天入口 |

---

## 3. 核心业务逻辑

### 3.1 知识库管理

**文档处理流程：**
```
上传文档 → TikaDocumentParser 提取文本
         → ChunkingStrategyFactory 创建分块器
         → FixedSizeTextChunker / StructureAwareTextChunker 分块
         → ChunkEmbeddingService 生成向量
         → Milvus/pgvector 存储向量 + PostgreSQL 存储元数据
```

- **支持的分块策略**：固定大小文本分块、结构感知文本分块
- **向量存储**：Milvus SDK 或 PostgreSQL pgvector
- **调度重摄入**：`KnowledgeDocumentScheduleJob` 支持定时重新摄入

### 3.2 意图识别

**意图树节点类型：**
- `KBIntentNode` - 知识库意图
- `MCPIntentNode` - MCP工具意图
- `SystemIntentNode` - 系统意图

**分类流程：**
```
用户问题 → QueryRewriteService 分解子问题
        → IntentClassifier 对每个子问题打分
        → IntentResolver 根据阈值选择匹配的意图节点
        → 返回 IntentResolveResult（包含 KB chunks + MCP tools）
```

### 3.3 多通道检索

`MultiChannelRetrievalEngine` 并行执行：
- `VectorGlobalSearchChannel` - 全局向量搜索
- `IntentDirectedSearchChannel` - 意图导向搜索

检索结果经过 `PostProcessor` 链：
1. **去重处理器** - 基于 chunk ID 去重
2. **Rerank 处理器** - 可选的 LLM Rerank

### 3.4 对话记忆

`ConversationMemoryService` 实现：
- **滑动窗口** - 保留最近 N 条消息
- **摘要生成** - 当窗口满时调用 LLM 生成摘要

### 3.5 模型路由与Failover

`RoutingLLMService` 核心逻辑：
```
1. 按优先级遍历候选模型
2. 探测模型健康状态（first-packet 超时检测）
3. ModelHealthStore 记录成功/失败（三态：健康/熔断恢复中/不可用）
4. 失败自动切换下一个模型
5. ProbeStreamBridge 检测模型切换时的首包延迟
```

---

## 4. MCP 服务器

### 4.1 两套实现

**独立 MCP 服务器（`mcp-server` 模块）：**
- JSON-RPC 2.0 over HTTP POST `/mcp`
- `MCPServerApplication` Spring Boot 启动类
- `DefaultMCPToolRegistry` 通过 `@PostConstruct` 自动发现 `MCPToolExecutor` Bean
- `MCPDispatcher` 路由 `initialize`、`tools/list`、`tools/call` 方法
- 内置执行器：`WeatherMCPExecutor`、`TicketMCPExecutor`、`SalesMCPExecutor`

**集成 MCP（`bootstrap/rag/core/mcp`）：**
- `MCPToolRegistry` 支持 `register/unregister`
- `HttpMCPClient` 调用远程 MCP 服务器
- `RemoteMCPToolExecutor` 包装远程调用
- `LLMMCPParameterExtractor` 使用 LLM 从用户问题中提取参数

### 4.2 RAG Pipeline 中的工具执行

```
IntentResolver.resolve() → 分类子问题到 MCPIntentNode
                       ↓
RetrievalEngine.retrieve() → 对每个 MCP 意图调用 MCPToolExecutor.execute(MCPRequest)
                           → 返回 MCPResponse
                           → ContextFormatter.formatMcpContext() 格式化工具响应为文本
                           → 拼入 LLM Prompt
```

---

## 5. 前端架构

### 5.1 技术栈

- **React 18** + **TypeScript** + **Vite**
- **Tailwind CSS** + **Radix UI**（shadcn-style 基础组件）
- **Zustand** 状态管理
- **React Router v6** 路由
- **React Hook Form** + **Zod** 表单验证
- **Recharts** 仪表盘图表
- **React Virtuoso** 虚拟化列表

### 5.2 状态管理

**authStore（Zustand）：**
- 认证状态（token、用户信息）
- 登录/登出逻辑

**chatStore（Zustand）：**
- 会话列表、消息列表
- 流式状态、deep thinking 开关
- `sendMessage()` 构建 SSE 请求，监听 `onMeta`、`onMessage`、`onThinking`、`onFinish`、`onCancel`、`onError` 事件
- `appendThinkingContent()` / `appendStreamContent()` 流式追加消息内容

### 5.3 路由结构

```
/login                    → LoginPage（已登录重定向到 /chat）
/chat                     → ChatPage（需认证）
/chat/:sessionId          → ChatPage（需认证）
/admin                    → AdminLayout（需 admin 角色）
  /admin/dashboard        → DashboardPage
  /admin/knowledge        → KnowledgeListPage
  /admin/knowledge/:id/documents → KnowledgeDocumentsPage
  /admin/knowledge/:id/documents/:docId/chunks → KnowledgeChunksPage
  /admin/intent-tree      → IntentTreePage
  /admin/intent-tree/edit → IntentEditPage
  /admin/ingestion        → IngestionPage
  /admin/traces           → RagTracePage
  /admin/traces/:id       → RagTraceDetailPage
  /admin/settings         → SystemSettingsPage
  /admin/sample-questions → SampleQuestionPage
  /admin/query-term-mapping → QueryTermMappingPage
  /admin/users            → UserListPage
```

### 5.4 SSE 流式处理

`hooks/useStreamResponse.ts` 核心逻辑：
- 使用 EventSource 或 fetch + ReadableStream 接收 SSE
- 解析 `data:` 行，映射到 chatStore 的回调
- 支持重试机制

---

## 6. 基础设施

### 6.1 数据库

- **PostgreSQL** + **MyBatis-Plus** ORM
- **pgvector** 扩展用于向量存储
- **MyMetaObjectHandler** 自动填充 `create_time` / `update_time`
- 关键表：`knowledge_base`、`knowledge_document`、`knowledge_chunk`、`conversation_message`、`conversation_session`、`ingestion_pipeline`、`ingestion_task`

### 6.2 消息队列

- **RocketMQ**（`rocketmq-spring-boot-starter`）
- `KnowledgeDocumentChunkConsumer` 消费文档块事件
- `KnowledgeDocumentChunkTransactionChecker` 处理事务检查
- `DelegatingTransactionListener` 委托事务监听器
- 用于异步文档块处理（事务消息确保一致性）

### 6.3 缓存与锁

- **Redis** + **Redisson** 分布式锁、限流、缓存
- **Sa-Token** 认证（Session + Token 模式）

### 6.4 文件存储

- **AWS S3 SDK v2**（`software.amazon.awssdk:s3`）
- `RestFSS3Config` 配置 S3 文件存储

---

## 7. 关键文件索引

| 文件 | 职责 |
|------|------|
| `StreamChatPipeline.java` | 编排完整 RAG 流水线 |
| `RetrievalEngine.java` | 协调 KB 检索和 MCP 工具执行，格式化上下文 |
| `IntentResolver.java` | 将子问题分类到意图树节点（KB/MCP/System） |
| `ConversationMemoryService.java` | 加载/追加会话历史（滑动窗口 + 摘要） |
| `RoutingLLMService.java` | 多模型健康探测 + Failover 路由 |
| `LLMService.java` / `EmbeddingService.java` | AI 模型访问核心接口 |
| `DefaultMCPToolRegistry.java` | 启动时自动发现并注册 MCP 工具执行器 |
| `MCPDispatcher.java` | 路由 JSON-RPC 请求（initialize/tools/list/tools/call） |
| `RAGChatController.java` | 暴露 SSE 端点 `/rag/v3/chat`，含幂等和限流 |
| `KnowledgeDocumentServiceImpl.java` | 处理文档上传、解析、分块、向量生成、存储 |
| `ChunkEmbeddingService.java` | 通过配置的 EmbeddingService 为文本块生成向量 |
| `chatStore.ts` | Zustand 管理聊天会话、消息、流式状态 |
| `router.tsx` | React Router v6 配置，含 auth guards |

---

## 8. 配置

主配置文件：`bootstrap/src/main/resources/application.yaml`

关键配置项：
```yaml
server:
  port: 9090
  context-path: /api/ragent

database:
  # PostgreSQL 配置

rag:
  vector:
    type: milvus | pgvector  # 向量库类型

ai:
  providers:
    ollama:
      base-url: http://localhost:11434
    bailian:
      api-key: xxx
    siliconflow:
      api-key: xxx
  selection:
    failure-threshold: 3
    open-duration: 30s
```

---

## 9. 构建与运行

### 后端（Java/Spring Boot）
```bash
# 全量构建
./mvnw clean package

# 跳过测试构建
./mvnw clean package -DskipTests

# 运行单个测试
./mvnw test -Dtest=ClassName#methodName -pl bootstrap

# 格式化代码（Spotless 自动在 compile 阶段运行）
./mvnw spotless:apply

# 启动应用
./mvnw spring-boot:run -pl bootstrap
```

### 前端（React/TypeScript）
```bash
cd frontend

# 安装依赖
npm install

# 开发服务器
npm run dev

# 生产构建
npm run build

# Lint
npm run lint

# 格式化
npm run format
```