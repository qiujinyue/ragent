# Ragent 项目简历亮点

## 1. 文档入库流程

基于 Pipeline 模式设计六阶段可插拔 ETL 引擎（Fetcher → Parser → Enhancer → Chunker → Enricher → Indexer），链表式节点调度支持条件分支执行与循环检测。实现固定滑窗与结构感知（Markdown 标题/代码块识别 + 贪心装箱）两种分块策略，分块后通过 LLM 自动为每个 chunk 生成摘要与关键词，提升检索召回精度。

## 2. 检索回答流程

构建意图导向 + 全局向量的双通道并行检索架构，二级线程池扇出（通道级 / 集合级），后处理链路完成去重（高分优先）与 Rerank 重排。流式回答阶段实现 ProbeStreamBridge 首包探测机制，模型响应超时自动熔断降级，配合 CAS 幂等关闭的 SseEmitterSender 实现线程安全 SSE 推送。会话记忆采用滑动窗口 + 异步摘要压缩（Redisson 分布式锁保护）。

## 3. 意图识别

设计三级树形意图体系（DOMAIN → CATEGORY → TOPIC），覆盖 KB 检索、MCP 工具调用、系统交互三种类型。通过结构化 Prompt 实现实体导向与主题导向两阶段判定，支持多子问题并行分类与"保底优先 + 全局排序"的配额控制。集成歧义检测服务，同名主题跨系统时自动引导用户消歧。意图树采用 Redis Cache-Aside 缓存，CRUD 时自动失效。

## 4. 性能优化

(1) 9 个专用线程池 + TtlExecutors 实现池隔离与跨线程上下文传播，核心池 CallerRunsPolicy 背压、流式池 AbortPolicy 快速失败；(2) 基于 Redisson 信号量 + Sorted Set + Lua 脚本实现分布式公平队列限流（聊天 50 并发 / 上传 10 并发）；(3) 三态熔断器（连续失败 2 次触发 OPEN，30s 冷却后 HALF_OPEN 单次探测），结合候选模型链自动 Failover。

## 5. MCP 工具调用

基于 MCP 协议实现 JSON-RPC 2.0 工具调用框架，Registry + Strategy 插件化架构通过 Spring 自动发现零配置注册执行器。已实现销售查询、工单查询、天气查询三个执行器，支持枚举参数校验与多维聚合。结合意图分类自动提取工具参数，将结构化结果注入 RAG 上下文，实现知识库检索与实时工具调用的混合问答。
