# RAG 完整链路流程详解（用户提问到接收回答）

## 整体调用链总览

```
前端 sendMessage()
  --> GET /rag/v3/chat?question=xxx&conversationId=xxx&deepThinking=xxx
    --> @IdempotentSubmit (幂等校验)
    --> RAGChatController.chat() 创建 SseEmitter
    --> RAGChatServiceImpl.streamChat()
      --> @ChatRateLimit AOP 切面拦截
        --> ChatQueueLimiter.enqueue() (Redis 全局并发限流 + 排队)
          --> 排队获得许可后提交到 chatEntryExecutor
            --> ChatRateLimitAspect.invokeWithTrace() (链路追踪)
              --> StreamChatPipeline.execute() (流水线编排)
                1. loadMemory()        -- 记忆加载
                2. rewriteQuery()      -- 查询改写+拆分
                3. resolveIntents()    -- 意图解析(并行)
                4. handleGuidance()    -- 歧义引导(短路)
                5. handleSystemOnly()  -- 纯系统响应(短路)
                6. retrieve()          -- 多通道检索(并行)
                7. streamRagResponse() -- Prompt构建 + LLM流式调用
                  --> RoutingLLMService.streamChat() (模型路由+降级)
                    --> ProbeStreamBridge (首包探测)
                    --> ChatClient.streamChat() (实际模型调用)
                      --> StreamChatEventHandler.onContent() (SSE推送到前端)
                        --> SseEmitterSender.sendEvent()
                          --> 前端 EventSource 收到事件
                --> onComplete() 记忆落库 + FINISH/DONE 事件
```

---

## 一、前端入口：SSE 流式请求发起

**文件**: `frontend/src/stores/chatStore.ts` -- `sendMessage()`

前端通过 `fetch` API 发起 GET 请求到 SSE 端点：

```typescript
const query = buildQuery({
  question: trimmed,
  conversationId: conversationId || undefined,
  deepThinking: deepThinkingEnabled ? true : undefined
});
const url = `${API_BASE_URL}/rag/v3/chat${query}`;
```

**文件**: `frontend/src/hooks/useStreamResponse.ts` -- `createStreamResponse()`

SSE 连接通过 `fetch` + `ReadableStream` 实现（非浏览器原生 EventSource），支持自定义 headers（Authorization token）、自动重试（默认 2 次，指数退避 600ms）。

### SSE 事件协议

| 事件名 | 数据 | 说明 |
|--------|------|------|
| `meta` | `{conversationId, taskId}` | 元信息，首个事件 |
| `message` | `{type: "response"/"think", delta: "..."}` | 增量内容 |
| `finish` | `{messageId, title}` | 完成，携带消息ID和标题 |
| `done` | `[DONE]` | 连接结束标记 |
| `cancel` | `{messageId, title}` | 取消 |
| `reject` | `{type: "response", delta: "系统繁忙..."}` | 限流拒绝 |

前端 `chatStore` 在收到 `meta` 事件后设置 `streamTaskId`，用于后续取消操作。取消时调用 `POST /rag/v3/stop?taskId=xxx`。

---

## 二、后端入口：RAGChatController

**文件**: `bootstrap/src/main/java/com/nageoffer/ai/ragent/rag/controller/RAGChatController.java`

```java
@IdempotentSubmit(
    key = "T(com.nageoffer.ai.ragent.framework.context.UserContext).getUserId()",
    message = "当前会话处理中，请稍后再发起新的对话"
)
@GetMapping(value = "/rag/v3/chat", produces = "text/event-stream;charset=UTF-8")
public SseEmitter chat(@RequestParam String question,
                       @RequestParam(required = false) String conversationId,
                       @RequestParam(required = false, defaultValue = "false") Boolean deepThinking) {
    SseEmitter emitter = new SseEmitter(0L);  // 0L = 无超时
    ragChatService.streamChat(question, conversationId, deepThinking, emitter);
    return emitter;
}
```

关键点：
- `@IdempotentSubmit` 基于 userId 做幂等校验，防止同一用户重复提交
- `SseEmitter(0L)` 表示永不超时
- Controller 立即返回 emitter，实际处理在异步线程中进行

---

## 三、限流排队：ChatRateLimit + ChatQueueLimiter

**文件**: `bootstrap/src/main/java/com/nageoffer/ai/ragent/rag/aop/ChatRateLimitAspect.java`

`@ChatRateLimit` 注解标记在 `RAGChatServiceImpl.streamChat()` 上，AOP 切面拦截后：

1. 从方法参数中提取 `question`, `conversationId`, `SseEmitter`
2. 调用 `chatQueueLimiter.enqueue()` 将请求放入排队
3. 排队获得许可后，通过 `invokeWithTrace()` 反射调用原方法

**文件**: `bootstrap/src/main/java/com/nageoffer/ai/ragent/rag/aop/ChatQueueLimiter.java`

全局并发限流基于 Redis 的分布式信号量 (`RPermitExpirableSemaphore`)：

- **信号量名**: `rag:global:chat`，并发数由 `rateLimitProperties.getGlobalMaxConcurrent()` 控制
- **排队队列**: Redis Sorted Set (`rag:global:chat:queue`)，按递增序号排列，保证 FIFO
- **Lua 原子抢占**: `queue_claim_atomic.lua` 确保只有队列头部的请求才能抢占信号量
- **轮询机制**: 请求入队后通过 `ScheduledExecutorService` 定时轮询（默认 200ms 间隔），同时通过 Redis Pub/Sub (`rag:global:chat:queue:notify`) 实现事件驱动唤醒
- **超时处理**: 超过 `globalMaxWaitSeconds` 后请求被拒绝，记录拒绝消息到对话历史，发送 `REJECT` 事件
- **资源释放**: `emitter.onCompletion/onTimeout/onError` 注册释放回调，确保信号量和队列清理

当排队获得许可后，任务提交到 `chatEntryExecutor` 线程池执行。

---

## 四、链路追踪创建

**文件**: `bootstrap/src/main/java/com/nageoffer/ai/ragent/rag/aop/ChatRateLimitAspect.java` -- `invokeWithTrace()`

在排队获得许可后、调用业务方法前：

```java
String traceId = IdUtil.getSnowflakeNextIdStr();
String taskId = IdUtil.getSnowflakeNextIdStr();
traceRecordService.startRun(RagTraceRunDO.builder()
    .traceId(traceId)
    .traceName("rag-stream-chat")
    .entryMethod("RAGChatServiceImpl#streamChat")
    .conversationId(conversationId)
    .taskId(taskId)
    .userId(UserContext.getUserId())
    .status("RUNNING")
    .startTime(new Date())
    .build());
RagTraceContext.setTraceId(traceId);
RagTraceContext.setTaskId(taskId);
```

使用 `TransmittableThreadLocal`（阿里 TTL）存储 traceId、taskId 和节点栈，确保在异步线程池中也能正确透传上下文。

后续流水线中的关键方法通过 `@RagTraceNode` 注解标记，AOP 切面会自动记录每个节点的执行耗时和状态。任务完成后通过 `traceRecordService.finishRun()` 更新状态为 SUCCESS/ERROR。

---

## 五、StreamChatPipeline 流水线编排

**文件**: `bootstrap/src/main/java/com/nageoffer/ai/ragent/rag/service/pipeline/StreamChatPipeline.java`

```
execute()
  1. loadMemory()        -- 记忆加载
  2. rewriteQuery()      -- 查询改写+拆分
  3. resolveIntents()    -- 意图解析(并行)
  4. handleGuidance()    -- 歧义引导(短路)
  5. handleSystemOnly()  -- 纯系统响应(短路)
  6. retrieve()          -- 多通道检索(并行)
  7. streamRagResponse() -- Prompt构建 + LLM流式调用
```

### 5.1 记忆加载（loadMemory）

**文件**: `bootstrap/.../rag/core/memory/DefaultConversationMemoryService.java`

并行加载摘要和历史记录：

```java
CompletableFuture<ChatMessage> summaryFuture = CompletableFuture.supplyAsync(
    () -> loadSummaryWithFallback(conversationId, userId));
CompletableFuture<List<ChatMessage>> historyFuture = CompletableFuture.supplyAsync(
    () -> loadHistoryWithFallback(conversationId, userId));
```

- `ConversationMemorySummaryService.loadLatestSummary()` -- 加载最新对话摘要（压缩后的早期对话）
- `ConversationMemoryStore.loadHistory()` -- 加载近期对话历史
- 两者合并：摘要作为 system 消息 + 历史消息列表

### 5.2 查询改写 + 拆分（rewriteQuery）

**文件**: `bootstrap/.../rag/core/rewrite/MultiQuestionRewriteService.java`

```java
@RagTraceNode(name = "query-rewrite-and-split", type = "REWRITE")
public RewriteResult rewriteWithSplit(String userQuestion, List<ChatMessage> history) {
    String normalizedQuestion = queryTermMappingService.normalize(userQuestion);
    return callLLMRewriteAndSplit(normalizedQuestion, userQuestion, history);
}
```

流程：
1. **归一化**: `QueryTermMappingService.normalize()` 做术语映射/纠错
2. **LLM 改写+拆分**: 调用 LLM 将问题改写为适合检索的形式，并拆分为多个子问题
   - temperature=0.1, topP=0.3（低随机性）
3. **解析**: LLM 返回 JSON `{"rewrite": "改写后的问题", "sub_questions": ["子问题1", "子问题2"]}`
4. **降级**: LLM 调用失败时，回退到归一化结果 + 规则拆分（按 `?？。；;\n` 分隔）

### 5.3 意图解析（resolveIntents）

**文件**: `bootstrap/.../rag/core/intent/IntentResolver.java`

```java
@RagTraceNode(name = "intent-resolve", type = "INTENT")
public List<SubQuestionIntent> resolve(RewriteResult rewriteResult) {
    List<CompletableFuture<SubQuestionIntent>> tasks = subQuestions.stream()
        .map(q -> CompletableFuture.supplyAsync(
            () -> new SubQuestionIntent(q, classifyIntents(q)),
            intentClassifyExecutor
        )).toList();
}
```

**并行执行点**: 每个子问题的意图分类在 `intentClassifyExecutor` 线程池中并行执行。

- `classifyIntents()` 对所有叶子意图节点做打分，过滤低于阈值的结果
- `capTotalIntents()` 在总意图数超限时做裁剪
- `mergeIntentGroup()` 将所有子问题的意图按 MCP/KB 分类

### 5.4 歧义引导（handleGuidance）

**文件**: `bootstrap/.../rag/core/guidance/IntentGuidanceService.java`

歧义检测条件：
- 只有 1 个子问题
- 存在 2+ 个 KB 意图
- 意图属于同一 topic
- 第二高分/最高分的比值 >= `ambiguityScoreRatio` 阈值

**短路处理**: 检测到歧义时，直接将引导 prompt 发送给前端，不进入检索阶段。

### 5.5 系统意图处理（handleSystemOnly）

纯系统内置意图（如打招呼、闲聊）直接生成响应，不进入检索阶段。

### 5.6 多通道检索（retrieve）

**文件**: `bootstrap/.../rag/core/retrieve/RetrievalEngine.java`

```java
@RagTraceNode(name = "retrieval-engine", type = "RETRIEVE")
public RetrievalContext retrieve(List<SubQuestionIntent> subIntents, int topK) {
    List<CompletableFuture<SubQuestionContext>> tasks = subIntents.stream()
        .map(si -> CompletableFuture.supplyAsync(
            () -> buildSubQuestionContext(si, topK), ragContextExecutor))
        .toList();
}
```

**并行执行点**: 子问题级别并行，每个子问题独立构建上下文。

`buildSubQuestionContext()` 对每个子问题：
1. KB 检索：调用 `MultiChannelRetrievalEngine.retrieveKnowledgeChannels()`
2. MCP 调用：调用 `executeMcpAndMerge()`（工具级并行，30 秒超时）

#### 多通道检索引擎

**文件**: `bootstrap/.../rag/core/retrieve/MultiChannelRetrievalEngine.java`

```
阶段1: 多通道并行检索
  ├── IntentDirectedSearchChannel (priority=1, 意图定向)
  └── VectorGlobalSearchChannel (priority=10, 全局兜底)

阶段2: PostProcessor 链
  ├── DeduplicationPostProcessor (order=1, 去重)
  └── RerankPostProcessor (order=10, 重排序)
```

### 5.7 流式 RAG 响应（streamRagResponse）

#### Prompt 构建

**文件**: `bootstrap/.../rag/core/prompt/RAGPromptService.java`

消息序列：
```
[system] 系统提示词（KB_ONLY / MCP_ONLY / MIXED）
[system] MCP 动态数据片段（如有）
[user]   KB 文档内容（如有）
[...]    对话历史
[user]   用户问题（多子问题时编号列出）
```

#### LLM 调用

**文件**: `infra-ai/.../chat/RoutingLLMService.java`

```java
@RagTraceNode(name = "llm-stream-routing", type = "LLM_ROUTING")
public StreamCancellationHandle streamChat(ChatRequest request, StreamCallback callback) {
    List<ModelTarget> targets = selector.selectChatCandidates(deepThinking);
    for (ModelTarget target : targets) {
        ProbeStreamBridge bridge = new ProbeStreamBridge(callback);
        StreamCancellationHandle handle = client.streamChat(request, bridge, target);
        ProbeResult result = bridge.awaitFirstPacket(60, TimeUnit.SECONDS);
        if (result.isSuccess()) {
            healthStore.markSuccess(target.id());
            return handle;
        }
        healthStore.markFailure(target.id());
        handle.cancel();
    }
}
```

**降级策略**:
- 逐个遍历候选模型，健康检查通过才尝试
- 首包超时 60 秒 --> 标记失败，切换下一个
- 调用异常 --> 标记失败，切换下一个
- 所有模型都失败 --> 抛出异常，通知前端

**健康状态**: 三态熔断器（可用/不可用/半开），连续失败达到阈值后熔断。

---

## 六、流式响应：SseEmitterSender

**文件**: `framework/.../web/SseEmitterSender.java`

线程安全的 SSE 发送封装：
- `AtomicBoolean closed` 保证幂等关闭
- `sendEvent(eventName, data)` 发送带命名事件
- `complete()` / `fail()` 使用 CAS 确保只关闭一次

**文件**: `bootstrap/.../rag/service/handler/StreamChatEventHandler.java`

`StreamChatEventHandler` 实现 `StreamCallback` 接口，是 LLM 流式输出到 SSE 的桥梁：

**初始化**:
```java
sender.sendEvent("meta", new MetaPayload(conversationId, taskId));
taskManager.register(taskId, sender, this::buildCompletionPayloadOnCancel);
```

**onContent(chunk)**:
```java
answer.append(chunk);
sendChunked("response", chunk);  // 按 messageChunkSize 分片发送
```

**onThinking(chunk)**:
```java
thinking.append(chunk);
sendChunked("think", chunk);
```

**onComplete**:
```java
// 1. 持久化 assistant 消息到记忆
messageId = memoryService.append(conversationId, userId, ChatMessage.assistant(answer, thinking, duration));
// 2. 发送 FINISH 事件
sender.sendEvent("finish", new CompletionPayload(messageId, title));
// 3. 发送 DONE 事件
sender.sendEvent("done", "[DONE]");
// 4. 注销任务、关闭连接
taskManager.unregister(taskId);
sender.complete();
```

---

## 七、记忆存储

**文件**: `bootstrap/.../rag/core/memory/DefaultConversationMemoryService.java`

对话结束后，在 `onComplete()` 回调中：

```java
public String append(String conversationId, String userId, ChatMessage message) {
    String messageId = memoryStore.append(conversationId, userId, message);
    summaryService.compressIfNeeded(conversationId, userId, message);
    return messageId;
}
```

1. `ConversationMemoryStore.append()` -- 将消息持久化到数据库
2. `ConversationMemorySummaryService.compressIfNeeded()` -- 当消息数超过阈值时，异步调用 LLM 生成摘要压缩

**取消场景**: 用户中途取消时，也会将已累积的 answer 内容持久化。

---

## 八、异步/并行执行点汇总

| 阶段 | 线程池 | 并行粒度 |
|------|--------|----------|
| 排队后入口 | `chatEntryExecutor` | 请求级 |
| 记忆加载（摘要+历史） | ForkJoinPool | 2 个并行任务 |
| 查询改写 | 同步（LLM 调用） | -- |
| 意图解析 | `intentClassifyExecutor` | 子问题级并行 |
| 子问题上下文构建 | `ragContextExecutor` | 子问题级并行 |
| 多通道检索 | `ragRetrievalExecutor` | 通道级并行 |
| 通道内部检索 | `ragInnerRetrievalExecutor` | 意图/collection 级并行 |
| MCP 工具调用 | `mcpBatchExecutor` | 工具级并行 |
| LLM 流式输出 | 模型回调线程 | -- |

所有线程池都使用 `TtlExecutors.getTtlExecutor()` 包装，确保 `RagTraceContext` 中的 traceId/taskId 在异步线程中正确透传。

---

## 九、完整数据流图

```
用户输入: "iPhone 15 和 iPhone 16 有什么区别？"
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│ 1. 限流排队 (ChatQueueLimiter)                           │
│    Redis 信号量 + 排队队列 + Lua 原子抢占               │
└─────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│ 2. 链路追踪创建 (invokeWithTrace)                        │
│    traceId = snowflake, RagTraceRunDO(status=RUNNING)   │
└─────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│ 3. 记忆加载 (loadMemory)                                 │
│    并行: loadSummary + loadHistory                       │
│    返回: List<ChatMessage> (摘要 + 近期历史)             │
└─────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│ 4. 查询改写 (rewriteQuery)                               │
│    归一化 → LLM 改写+拆分 → 降级(规则拆分)              │
│    返回: RewriteResult("iPhone15vs16对比",               │
│            ["iPhone15规格", "iPhone16规格", "差异对比"])  │
└─────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│ 5. 意图解析 (resolveIntents) [并行]                      │
│    子问题1 → IntentClassifier → NodeScore(hr_kb, 0.85)  │
│    子问题2 → IntentClassifier → NodeScore(hr_kb, 0.90)  │
│    子问题3 → IntentClassifier → NodeScore(hr_kb, 0.80)  │
└─────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│ 6. 歧义检测 (handleGuidance)                             │
│    无歧义 → 继续                                        │
└─────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│ 7. 多通道检索 (retrieve) [并行]                          │
│    ├── IntentDirectedSearchChannel → hr_kb collection    │
│    │     (子问题1 + 子问题2 + 子问题3 并行检索)          │
│    └── VectorGlobalSearchChannel (禁用, 有意图)          │
│                                                          │
│    PostProcessor 链:                                     │
│    ├── DeduplicationPostProcessor → 去重                 │
│    └── RerankPostProcessor → 重排序                      │
│                                                          │
│    返回: RetrievalContext(kbContext: List<RetrievedChunk>)│
└─────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│ 8. Prompt 构建 (RAGPromptService)                        │
│    [system] RAG 系统提示词                               │
│    [user]   KB 文档内容 (iPhone15/16 规格文档)           │
│    [...]    对话历史                                     │
│    [user]   用户问题 (编号列出3个子问题)                 │
└─────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│ 9. LLM 流式调用 (RoutingLLMService)                      │
│    ModelSelector → 选择候选模型列表                      │
│    遍历候选:                                              │
│      healthStore.allowCall? → ProbeStreamBridge          │
│      → ChatClient.streamChat() → 等待首包(60s)          │
│      成功 → 返回handle / 失败 → 切换下一个              │
└─────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│ 10. 流式响应 (StreamChatEventHandler → SseEmitterSender) │
│     onContent("iPhone") → SSE: event:message             │
│     onContent(" 15")    → SSE: event:message             │
│     onContent(" ...")   → SSE: event:message             │
│     ...                                                  │
│     onComplete → 持久化记忆 → event:finish → event:done  │
└─────────────────────────────────────────────────────────┘
    │
    ▼
前端 EventSource 收到事件，逐步渲染回答
```