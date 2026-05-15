# Ragent 学习计划

## 一、RAG 核心链路

### 1.1 完整流程概览

**入口**: `RAGChatController.chat()` → `StreamChatPipeline.execute()`

```
用户提问
    │
    ▼
1. loadMemory()        ─ 并行加载对话摘要(LLM) + 历史记录(JDBC)
2. rewriteQuery()      ─ LLM改写问题 + 拆分复合问题
3. resolveIntents()    ─ 并行分类每个子问题到意图树叶子节点
4. handleGuidance()    ─ 歧义检测 → 引导用户澄清 → 短路
5. handleSystemOnly()  ─ 系统响应(greeting/about) → 短路
6. retrieve()          ─ 多通道检索(KB) + MCP工具调用
7. handleEmptyRetrieval() ─ 空结果 → "未检索到..." → 短路
8. streamRagResponse() ─ 构建Prompt → LLM流式输出 → SSE推送
```

### 1.2 意图识别 (Intent Classification)

- `DefaultIntentClassifier` 将整棵意图树的叶子节点（含描述、示例）全部塞进 system prompt
- LLM 返回 JSON 评分列表，匹配多个叶子节点
- `NodeScoreFilters` 过滤出 KB 类型和 MCP 类型的节点

**关键类**:
- `IntentClassifier` - 意图分类接口
- `DefaultIntentClassifier` - LLM打分实现
- `IntentTreeFactory` - 构建默认意图树
- `IntentTreeCacheManager` - Redis缓存意图树
- `IntentResolver` - 并行分类编排
- `NodeScore` / `NodeScoreFilters` - 分类结果过滤工具

### 1.3 多通道检索 (Multi-Channel Retrieval)

- `IntentDirectedSearchChannel` (优先级1) — 根据匹配的意图节点定向搜索对应 Milvus collection
- `VectorGlobalSearchChannel` (优先级10) — 全局向量搜索兜底
- `RerankPostProcessor` — 重排序
- 后处理器链：`DeduplicationPostProcessor` → `RerankPostProcessor`

**关键类**:
- `MultiChannelRetrievalEngine` - 并行执行多个SearchChannel
- `RetrievalEngine` - 协调KB检索和MCP工具调用
- `SearchChannel` - 检索通道接口
- `SearchResultPostProcessor` - 后处理器接口

### 1.4 模型路由与容错 (Model Routing)

- `RoutingLLMService` + `ModelRoutingExecutor` 实现三态熔断器
- `ProbeStreamBridge` 实现首包探测，模型切换时保证 SSE 不丢数据
- 失败次数达到阈值自动熔断，冷却期后进入半开状态放行探测请求

**关键类**:
- `RoutingLLMService` - 模型路由服务
- `ModelRoutingExecutor` - 路由执行器
- `ModelHealthStore` - 模型健康状态存储（三态熔断器）
- `ProbeStreamBridge` - 首包探测桥接器
- `StreamCallback` - 流式回调接口

### 1.5 会话记忆 (Memory)

- `JdbcConversationMemoryStore` 存储原始消息
- `JdbcConversationMemorySummaryService` 实现记忆压缩（超过 N 轮触发 LLM 摘要）
- 并行加载摘要和历史，摘要附加在历史前面

**关键类**:
- `ConversationMemoryService` - 记忆服务接口
- `DefaultConversationMemoryService` - 并行加载实现
- `JdbcConversationMemoryStore` - JDBC存储实现
- `JdbcConversationMemorySummaryService` - 摘要压缩实现

---

## 二、非核心功能设计

### 2.1 文档入库流水线 (Ingestion Pipeline)

节点链式编排，通过 `nextNodeId` 串联：

```
FetcherNode → ParserNode → EnhancerNode → ChunkerNode → EnricherNode → IndexerNode
  获取文件       解析文本      全文增强       分块         chunk增强     写入Milvus
```

- `ConditionEvaluator` 支持条件跳过
- 每节点独立日志，可精确追踪失败步骤
- 配置存储在数据库，支持条件执行和输出链式传递

**关键类**:
- `IngestionEngine` - 流水线执行引擎
- `IngestionNode` - 节点接口（模板方法模式）
- `IngestionContext` - 节点间传递的上下文
- `PipelineDefinition` / `NodeConfig` - 流水线配置

### 2.2 限流 (Rate Limiting)

- `ChatRateLimitAspect` AOP 拦截 SSE 请求
- Redis ZSET 排队 + `RPermitExpirableSemaphore` 信号量控制并发
- Lua 脚本原子化抢占队列位置
- 8个专用线程池，全部用 `TtlExecutors` 包装保证上下文透传

**关键类**:
- `ChatRateLimitAspect` - AOP切面
- `ChatQueueLimiter` - 队列限流器
- `ThreadPoolExecutorConfig` - 线程池配置

### 2.3 MCP 工具集成

- `mcp-server` 是独立 Spring Boot 应用，暴露 `/mcp` JSON-RPC 端点
- `HttpMCPClient` 在 bootstrap 侧通过 Streamable HTTP 调用远程 MCP 工具
- `RemoteMCPToolExecutor` 包装远程工具，自动注册到 `MCPToolRegistry`

**关键类**:
- `MCPToolRegistry` - 工具注册表（注册表模式）
- `MCPToolExecutor` - 工具执行器接口
- `HttpMCPClient` - HTTP客户端
- `MCPDispatcher` / `MCPEndpoint` - JSON-RPC端点

### 2.4 意图树管理

- 3级结构：DOMAIN → CATEGORY → TOPIC
- 3种类型：KB（知识库检索）、MCP（实时工具调用）、SYSTEM（系统响应）
- Redis 缓存，7天TTL，CRUD 时自动失效

**关键类**:
- `IntentNode` - 意图节点领域模型
- `IntentTreeService` - 意图树CRUD服务
- `IntentNodeRegistry` - 节点注册表

### 2.5 Admin 仪表盘

- KPI：用户数、会话数、消息数（含24小时增量）
- 性能：平均/P95延迟、成功率、慢请求比例
- 趋势：时间序列的会话量、消息量、延迟、质量指标

**关键类**:
- `DashboardController` - 仪表盘API
- `DashboardServiceImpl` - 指标计算实现

---

## 三、核心设计模式

| 模式 | 典型应用 |
|------|----------|
| 策略模式 | `SearchChannel`、`PostProcessor`、`MCPToolExecutor` 可插拔 |
| 注册表模式 | `MCPToolRegistry`、`IntentNodeRegistry` 自动发现 |
| 模板方法 | `IngestionNode` 基类定义执行骨架 |
| 装饰器 | `ProbeBufferingCallback` 增加首包探测能力 |
| 责任链 | 后处理器链、模型降级链 |
| AOP | `@RagTraceNode` 链路追踪、`@ChatRateLimit` 限流 |
| 工厂模式 | `IntentTreeFactory`、`StreamCallbackFactory` |
| 观察者模式 | `StreamCallback` 流式事件通知 |

---

## 四、学习路径建议

### 第一阶段：项目结构 (1天)
1. 理解四层 Maven 模块架构：bootstrap / framework / infra-ai / mcp-server
2. 理解 application.yaml 配置结构
3. 理解前后端交互方式（SSE）

### 第二阶段：Framework 层 (2天)
1. `SseEmitterSender` - 线程安全 SSE 发送封装
2. `RagTraceNode` / `RagTraceAspect` - 全链路追踪
3. `IdempotentSubmitAspect` / `IdempotentConsumeAspect` - 幂等性控制
4. `SnowflakeIdInitializer` - 分布式ID生成
5. 理解 UserContext 跨线程透传机制

### 第三阶段：Infra-AI 层 (2天)
1. `ChatClient` 接口及三家实现（百炼/Ollama/SiliconFlow）
2. `RoutingLLMService` 模型路由与三态熔断器
3. `ProbeStreamBridge` 首包探测原理
4. `EmbeddingClient` / `RerankClient` 接口及实现

### 第四阶段：RAG 核心链路 (3天)
1. `StreamChatPipeline` 流程编排
2. 意图识别：意图树结构 → LLM分类 → 结果过滤
3. 多通道检索：并行执行 → 后处理链 → 重排序
4. 会话记忆：滑动窗口 + 摘要压缩
5. Prompt 构建与场景选择（KB_ONLY / MCP_ONLY / MIXED）

### 第五阶段：MCP 工具集成 (1天)
1. JSON-RPC 2.0 协议理解
2. mcp-server 端点实现
3. HttpMCPClient 调用流程
4. 工具注册与自动发现

### 第六阶段：文档入库流水线 (1天)
1. `IngestionEngine` 链式执行
2. 各类节点实现原理
3. 节点配置与条件执行

### 第七阶段：工程化特性 (1天)
1. 限流实现：Redis ZSET + 信号量 + Lua脚本
2. 线程池配置与上下文透传
3. 三态熔断器实现
4. Admin 仪表盘数据来源

---

## 五、关键文件索引

### RAG 核心
| 文件 | 作用 |
|------|------|
| `StreamChatPipeline.java` | RAG流水线编排 |
| `DefaultIntentClassifier.java` | 意图分类实现 |
| `MultiChannelRetrievalEngine.java` | 多通道检索引擎 |
| `RoutingLLMService.java` | 模型路由服务 |
| `ProbeStreamBridge.java` | 首包探测 |
| `DefaultConversationMemoryService.java` | 会话记忆 |

### Framework 层
| 文件 | 作用 |
|------|------|
| `SseEmitterSender.java` | 线程安全SSE |
| `RagTraceAspect.java` | 链路追踪AOP |
| `IdempotentSubmitAspect.java` | 幂等提交 |
| `UserContext.java` | 用户上下文 |

### Infra-AI 层
| 文件 | 作用 |
|------|------|
| `AbstractOpenAIStyleChatClient.java` | OpenAI风格聊天客户端基类 |
| `ModelHealthStore.java` | 三态熔断器 |
| `RoutingLLMService.java` | 模型路由 |

### 文档入库
| 文件 | 作用 |
|------|------|
| `IngestionEngine.java` | 流水线引擎 |
| `ChunkerNode.java` | 分块节点 |
| `IndexerNode.java` | 索引节点 |
