# Ragent 项目简历亮点

## 亮点一：文档入库流程（ETL Pipeline）

### 简历描述

> 设计并实现基于 Pipeline 模式的文档 ETL 入库引擎，支持 FETCHER → PARSER → ENHANCER → CHUNKER → ENRICHER → INDEXER 六阶段可插拔节点编排。采用链表式节点调度（`nextNodeId` 指针驱动），支持条件分支执行（SpEL 表达式 + JSON 规则 DSL）、循环检测和节点级耗时日志追踪。针对中文文档场景，实现结构感知分块策略（StructureAwareTextChunker），通过标题/代码块/段落识别 + 贪心装箱算法，在保证语义完整性的同时将 chunk 利用率提升；分块后通过 LLM 增强节点为每个 chunk 自动生成摘要、关键词和元数据，提升检索召回精度。

### 技术细节备答

- **两种分块策略**：固定滑窗（边界感知，优先在句号/换行处切割）和结构感知（Markdown 标题/代码块识别，贪心装箱 + 微块合并）
- **Fetcher 策略模式**：支持本地文件、HTTP URL、S3、飞书四种来源
- **LLM 增强节点**：Enhancer 支持关键词提取、问题生成、元数据提取四种增强任务；Enricher 为每个 chunk 独立生成摘要和关键词
- **节点条件执行**：`ConditionEvaluator` 支持布尔字面量、SpEL 表达式、嵌套 `all/any/not` 规则
- **分块配置**：FixedSizeOptions（chunkSize=512, overlapSize=128）、TextBoundaryOptions（targetChars=1400, maxChars=1800, minChars=600）
- **可观测性**：每个节点执行计时 + 输出快照，持久化为 `IngestionTaskNodeDO` 记录

---

## 亮点二：检索回答流程（Multi-Channel RAG）

### 简历描述

> 构建多通道并行检索引擎，设计意图导向检索通道（IntentDirectedSearchChannel，优先级 1）与全局向量检索通道（VectorGlobalSearchChannel，优先级 10，兜底）的双通道架构。各通道通过 `CompletableFuture` 二级线程池扇出（外层通道级并行，内层集合级并行），结合 Dedup 去重（高分优先）+ Rerank 重排的后处理链路。流式回答阶段，实现 `ProbeStreamBridge` 首包探测机制：在模型切换场景下缓存首个数据包，60 秒内未收到响应自动熔断并降级到下一候选模型，配合 `SseEmitterSender`（CAS 幂等关闭）实现线程安全的 SSE 流式推送。

### 技术细节备答

- **检索后处理链**：`DeduplicationPostProcessor`(order=1) → `RerankPostProcessor`(order=10)，基于链式责任模式
- **去重逻辑**：按通道优先级遍历，`LinkedHashMap` 保序，重复 chunk 取高分版本
- **模型路由**：`ModelRoutingExecutor.executeWithFallback()` 遍历候选模型，结合 `ModelHealthStore` 三态熔断器（CLOSED/OPEN/HALF_OPEN）自动跳过不健康模型
- **ProbeStreamBridge**：`CompletableFuture` 探测 + `synchronized` 缓冲区 + `volatile committed` 标志，实现首包检测与无缝切换
- **会话记忆**：滑动窗口（最近 8 轮）+ 异步摘要压缩（Redisson 分布式锁保护，触发阈值 9 轮）
- **Prompt 组装**：根据意图类型自动选择 KB_ONLY / MCP_ONLY / MIXED 模板，支持节点级自定义模板覆盖
- **SSE 线程安全**：`AtomicBoolean closed` + `compareAndSet` 幂等关闭，异常不抛出避免响应冲突

---

## 亮点三：意图识别（Tree-Based Intent Classification）

### 简历描述

> 设计基于树形意图体系的 LLM 意图分类系统，支持 DOMAIN → CATEGORY → TOPIC 三级知识域划分，覆盖知识库检索（KB）、MCP 工具调用（MCP）、系统交互（SYSTEM）三种意图类型。通过 StringTemplate 构建结构化 Prompt，采用"实体导向 vs 主题导向"两阶段判定策略，LLM 输出评分 >0.8 视为强匹配，<0.4 自动过滤。支持多子问题并行分类（专用线程池 + CompletableFuture 扇出），并通过"保底优先 + 全局分数排序"的配额分配算法控制最大意图数（默认 3 个），避免检索过载。集成歧义检测服务，当同一主题名在多个系统下存在时自动引导用户消歧。

### 技术细节备答

- **意图树结构**：`IntentNode` 包含 id/name/description/level/kind/children/fullPath/examples 等字段，支持 KB/MCP/SYSTEM 三种类型
- **分类流程**：加载意图树（Redis 缓存 → DB 兜底 → 回写缓存）→ 构建叶子节点 Prompt → LLM 评分（temp=0.1, topP=0.3）→ JSON 解析 → 分数过滤
- **Prompt 策略**：两阶段判定（实体导向 vs 主题导向）、数量控制（默认 1 个，歧义场景最多 3 个）、系统约束（提及特定系统时仅匹配子树）
- **多子问题处理**：`IntentResolver` 对多个子问题并行分类，`capTotalIntents()` 算法：保底每题最高分意图 + 全局分数排序填充剩余配额
- **缓存策略**：Redis Cache-Aside 模式（7 天 TTL），每次 CRUD 操作自动清缓存
- **歧义检测**：`IntentGuidanceService` 按归一化名称分组，检测同名主题在多系统下的情况，生成消歧 Prompt
- **双接口设计**：`DefaultIntentClassifier` 同时实现 `IntentClassifier` 和 `IntentNodeRegistry`，解耦分类与节点查询

---

## 亮点四：性能评估与优化（High-Availability Infrastructure）

### 简历描述

> 构建多层次高可用保障体系：(1) **线程池隔离** — 设计 9 个专用线程池（检索/意图分类/记忆摘要/模型流式输出等），采用 `TtlExecutors` 包装实现 TransmittableThreadLocal 跨线程上下文传播，核心池使用 CallerRunsPolicy 背压、流式池使用 AbortPolicy 快速失败；(2) **分布式限流** — 基于 Redisson 信号量 + Sorted Set 实现全局聊天并发控制（默认 50 并发）和文档上传限流（默认 10 并发），配合 Lua 脚本实现原子抢占 + Pub/Sub 唤醒的公平队列；(3) **模型熔断降级** — 实现三态熔断器（连续失败 2 次触发 OPEN，30 秒冷却后 HALF_OPEN 仅放行单次探测），结合优先级排序的候选模型链自动 Failover；(4) **幂等保障** — 提交端 + 消费端双维度幂等（Redis + 数据库双重校验）。

### 技术细节备答

- **线程池分层**：外层 `ragRetrievalExecutor`(CPU/CPU*2) 通道级并行，内层 `ragInnerRetrievalExecutor`(CPU*2/CPU*4) 集合级并行，防止通道间饥饿
- **拒绝策略选择**：CallerRunsPolicy（池 1-6，背压降级）vs AbortPolicy（池 7-9，下游有限流保护，快速失败）
- **分布式限流**：Redisson `RPermitExpirableSemaphore` + `RScoredSortedSet` 公平队列 + Lua 原子抢占脚本 + `RTopic` Pub/Sub 唤醒
- **限流拒绝体验**：将用户问题记入会话记忆，通过 SSE 推送 REJECT 事件，前端展示友好提示
- **熔断器线程安全**：所有状态变更通过 `ConcurrentHashMap.compute()` 原子操作，无需显式锁
- **模型选择策略**：firstChoice 固定优先 → priority 排序 → ID 字母序，自动过滤不可用/禁用/不支持深度思考的候选
- **TTL 上下文传播**：所有线程池通过 `TtlExecutors.getTtlExecutor()` 包装，保证用户上下文和 TraceID 跨线程传递

---

## 亮点五：MCP 工具调用（Model Context Protocol Integration）

### 简历描述

> 基于 MCP（Model Context Protocol）标准协议实现工具调用框架，设计 Registry + Strategy 的插件化架构：`MCPToolExecutor` 策略接口定义工具契约，`DefaultMCPToolRegistry` 通过 Spring 自动发现机制零配置注册所有执行器，`MCPDispatcher` 统一处理 JSON-RPC 2.0 协议的 `initialize` / `tools/list` / `tools/call` 三种方法。已实现销售数据查询、工单查询、天气查询三个业务执行器，支持枚举参数校验、多维度聚合查询（汇总/排名/明细/趋势）。结合意图分类系统，当识别到 MCP 类型意图时自动提取工具参数（LLM 参数抽取 + 模板化 Prompt），调用对应执行器并将结构化结果注入 RAG 上下文，实现"知识库检索 + 实时工具调用"的混合问答能力。

### 技术细节备答

- **协议规范**：JSON-RPC 2.0 over HTTP，支持 notification（id=null 返回 204），标准错误码（-32601 Method Not Found 等）
- **工具定义自动生成**：`MCPToolDefinition` → `MCPToolSchema` 转换，包含 `inputSchema` 的 properties/required/enum 描述
- **注册机制**：`DefaultMCPToolRegistry` 构造器注入 `List<MCPToolExecutor>`，`@PostConstruct` 自动注册到 `ConcurrentHashMap`
- **混合场景处理**：`RAGPromptService` 根据 `PromptScene.MIXED` 自动选择 `MCP_KB_MIXED_PROMPT_PATH` 模板
- **参数抽取**：通过 `paramPromptTemplate` 让 LLM 从用户自然语言中提取结构化参数
- **执行器设计**：枚举参数约束（region/status/priority 等）、确定性 Mock 数据（种子随机数）、日内缓存
- **端点暴露**：`MCPEndpoint` 单一 `POST /mcp` 入口，`MCPDispatcher` switch 路由三种 JSON-RPC 方法
