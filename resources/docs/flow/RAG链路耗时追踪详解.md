# RAG 链路耗时追踪详解

## 整体架构概览

链路追踪系统采用**注解驱动 + AOP 切面 + TTL 跨线程上下文传递**的方案，核心分为四层：

```
[注解层]  @RagTraceRoot / @RagTraceNode     (framework 模块)
[切面层]  RagTraceAspect / ChatRateLimitAspect (bootstrap 模块 AOP)
[上下文]  RagTraceContext (TransmittableThreadLocal) (framework 模块)
[持久层]  RagTraceRunDO + RagTraceNodeDO + MyBatis Plus Mapper (bootstrap 模块 DAO)
[查询层]  RagTraceQueryService + RagTraceController + 前端页面
```

---

## 一、注解定义

### @RagTraceNode

**文件**: `framework/src/main/java/com/nageoffer/ai/ragent/framework/trace/RagTraceNode.java`

```java
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RagTraceNode {
    String name() default "";       // 节点名称（用于展示）
    String type() default "METHOD"; // 节点类型（用于分组统计）
}
```

### @RagTraceRoot

**文件**: `framework/src/main/java/com/nageoffer/ai/ragent/framework/trace/RagTraceRoot.java`

```java
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RagTraceRoot {
    String name() default "";                          // 链路名称
    String conversationIdArg() default "conversationId"; // 会话ID参数名
    String taskIdArg() default "taskId";                 // 任务ID参数名
}
```

两个注解构成**根节点 + 子节点**的层级关系。`@RagTraceRoot` 标记入口方法，创建整条链路；`@RagTraceNode` 标记链路中的各步骤。

---

## 二、AOP 切面实现

**文件**: `bootstrap/src/main/java/com/nageoffer/ai/ragent/rag/aop/RagTraceAspect.java`

切面通过 `@Around` 环绕通知处理两个注解，优先级为 `Ordered.HIGHEST_PRECEDENCE + 10`，确保在其他切面之前执行。

### 2.1 Root 切面逻辑（`aroundRoot`）

```
1. 检查 rag.trace.enabled 配置，未启用直接放行
2. 检查当前线程是否已在链路中（RagTraceContext.getTraceId() 非空），避免重复创建
3. 生成 traceId（雪花ID）
4. 从方法参数中解析 conversationId 和 taskId（通过反射匹配参数名）
5. 插入 RagTraceRunDO 记录（status=RUNNING）
6. 设置 RagTraceContext.setTraceId(traceId)
7. 执行目标方法
8. 成功则 finishRun(SUCCESS)，异常则 finishRun(ERROR) 并截断错误信息
9. finally 中调用 RagTraceContext.clear() 清理上下文
```

### 2.2 Node 切面逻辑（`aroundNode`）

```
1. 检查 enabled 配置
2. 获取当前 traceId，若为空（不在链路中）直接放行
3. 生成 nodeId（雪花ID），获取 parentNodeId 和 depth
4. 插入 RagTraceNodeDO 记录（status=RUNNING）
5. RagTraceContext.pushNode(nodeId) 入栈
6. 执行目标方法
7. 成功则 finishNode(SUCCESS)，异常则 finishNode(ERROR)
8. finally 中 RagTraceContext.popNode() 出栈
```

关键设计：通过**节点栈（Deque）**实现父子关系。`parentNodeId = RagTraceContext.currentNodeId()` 取栈顶即为当前节点的父节点，`depth = RagTraceContext.depth()` 记录嵌套深度。

---

## 三、Trace 上下文传递机制（TTL）

**文件**: `framework/src/main/java/com/nageoffer/ai/ragent/framework/trace/RagTraceContext.java`

```java
public final class RagTraceContext {
    private static final TransmittableThreadLocal<String> TRACE_ID = new TransmittableThreadLocal<>();
    private static final TransmittableThreadLocal<String> TASK_ID = new TransmittableThreadLocal<>();
    private static final TransmittableThreadLocal<Deque<String>> NODE_STACK = new TransmittableThreadLocal<>();
    // ... pushNode / popNode / currentNodeId / depth / clear
}
```

使用阿里巴巴的 **TransmittableThreadLocal (TTL)** 替代 JDK 原生 `ThreadLocal`。TTL 的核心能力是：当任务提交到线程池时，自动将父线程的上下文值捕获并传递到子线程，任务执行完毕后自动还原。

### 线程池包装

项目中所有 8 个业务线程池全部用 `TtlExecutors.getTtlExecutor(executor)` 包装：

- `mcp-batch` / `rag-context-assembly` / `multi-retrieval` / `internal-retrieval` / `intent-classify` / `memory-summary` / `model-stream` / `chat-entry` / `knowledge-chunk`

这确保了即使 RAG 流程中的检索、意图分类、模型调用等步骤在异步线程池中执行，`RagTraceContext` 中的 `traceId`、`taskId` 和 `NODE_STACK` 都能正确透传，使得子线程中的 `@RagTraceNode` 方法也能正确关联到同一条链路。

---

## 四、链路追踪数据存储

### 4.1 数据库表结构（PostgreSQL）

#### t_rag_trace_run -- 链路运行记录（一次完整请求一条）

| 字段 | 类型 | 说明 |
|------|------|------|
| id | VARCHAR(20) PK | 雪花ID |
| trace_id | VARCHAR(64) UNIQUE | 全局链路ID |
| trace_name | VARCHAR(128) | 链路名称 |
| entry_method | VARCHAR(256) | 入口方法全限定名 |
| conversation_id | VARCHAR(20) | 会话ID |
| task_id | VARCHAR(20) | 任务ID |
| user_id | VARCHAR(20) | 用户ID |
| status | VARCHAR(16) | RUNNING/SUCCESS/ERROR |
| error_message | VARCHAR(1000) | 错误信息（截断） |
| start_time / end_time | TIMESTAMP(3) | 起止时间（毫秒精度） |
| duration_ms | BIGINT | 耗时毫秒 |
| extra_data | TEXT | 扩展JSON |
| deleted | SMALLINT | 逻辑删除 |

#### t_rag_trace_node -- 链路节点记录（每个步骤一条）

| 字段 | 类型 | 说明 |
|------|------|------|
| trace_id | VARCHAR(20) | 关联链路ID |
| node_id | VARCHAR(20) UNIQUE | 节点ID |
| parent_node_id | VARCHAR(20) | 父节点ID（构建树形结构） |
| depth | INTEGER | 嵌套深度 |
| node_type | VARCHAR(16) | 节点类型（REWRITE/INTENT/RETRIEVE/LLM_PROVIDER 等） |
| node_name | VARCHAR(128) | 节点名称 |
| class_name / method_name | VARCHAR | 类名和方法名 |
| status / error_message / start_time / end_time / duration_ms | | 同 run 表 |

### 4.2 Mapper 层

- `RagTraceRunMapper extends BaseMapper<RagTraceRunDO>` -- 直接继承 MyBatis Plus 的 BaseMapper
- `RagTraceNodeMapper extends BaseMapper<RagTraceNodeDO>` -- 同上

均无自定义 SQL，完全依赖 MyBatis Plus 内置 CRUD。

### 4.3 两阶段写入模式

```java
// 1. INSERT status=RUNNING
public void startRun(RagTraceRunDO run) {
    runMapper.insert(run);
}

// 2. 方法结束后 UPDATE 为 SUCCESS/ERROR
public void finishRun(String traceId, String status, String errorMessage, ...) {
    runMapper.update(update, Wrappers.lambdaUpdate(RagTraceRunDO.class)
            .eq(RagTraceRunDO::getTraceId, traceId));
}
```

采用**先插入 RUNNING 记录、方法结束后 UPDATE 为 SUCCESS/ERROR** 的两阶段写入模式，保证即使异常也能记录部分数据。

---

## 五、@RagTraceRoot 的实际触发点

`@RagTraceRoot` 注解在代码中**尚未被直接使用**。实际的链路根节点创建由 `ChatRateLimitAspect` 手动完成。

**文件**: `bootstrap/.../rag/aop/ChatRateLimitAspect.java` -- 这是实际的链路入口

```java
@Around("@annotation(com.nageoffer.ai.ragent.rag.aop.ChatRateLimit)")
public Object limitStreamChat(ProceedingJoinPoint joinPoint) throws Throwable {
    // ... 排队限流逻辑
    chatQueueLimiter.enqueue(question, actualConversationId, emitter, () -> {
        invokeWithTrace(method, target, args, question, actualConversationId, emitter);
    });
    return null;
}

private void invokeWithTrace(...) {
    String traceId = IdUtil.getSnowflakeNextIdStr();
    String taskId = IdUtil.getSnowflakeNextIdStr();
    traceRecordService.startRun(RagTraceRunDO.builder()
            .traceId(traceId)
            .traceName("rag-stream-chat")
            .entryMethod(method.getDeclaringClass().getName() + "#" + method.getName())
            .conversationId(conversationId)
            .taskId(taskId)
            .userId(UserContext.getUserId())
            .status(STATUS_RUNNING)
            .extraData(StrUtil.format("{\"questionLength\":{}}", StrUtil.length(question)))
            .build());

    RagTraceContext.setTraceId(traceId);
    RagTraceContext.setTaskId(taskId);
    try {
        method.invoke(target, args);
        traceRecordService.finishRun(traceId, STATUS_SUCCESS, ...);
    } catch (Throwable ex) {
        traceRecordService.finishRun(traceId, STATUS_ERROR, ...);
    } finally {
        RagTraceContext.clear();
    }
}
```

设计原因：由于 SSE 流式对话需要先排队（`ChatQueueLimiter`），实际执行在排队回调中，因此链路创建不能用简单的 `@RagTraceRoot` 注解，而是集成在限流切面中手动管理。

---

## 六、@RagTraceNode 的实际使用点

共 **11 个方法**标注了 `@RagTraceNode`，覆盖 RAG 流程的各个关键阶段：

| 类 | 方法 | name | type | 说明 |
|----|------|------|------|------|
| `MultiQuestionRewriteService` | `rewrite()` | query-rewrite | REWRITE | 查询改写 |
| `MultiQuestionRewriteService` | `rewriteWithSplit()` | query-rewrite-and-split | REWRITE | 查询改写+子问题拆分 |
| `IntentResolver` | `resolve()` | intent-resolve | INTENT | 意图解析 |
| `MultiChannelRetrievalEngine` | `retrieveKnowledgeChannels()` | multi-channel-retrieval | RETRIEVE_CHANNEL | 多通道并行检索 |
| `RetrievalEngine` | `retrieve()` | retrieval-engine | RETRIEVE | 检索引擎主入口 |
| `RoutingLLMService` | `chat()` | llm-chat-routing | LLM_ROUTING | LLM 路由（同步） |
| `RoutingLLMService` | `streamChat()` | llm-stream-routing | LLM_ROUTING | LLM 路由（流式） |
| `BaiLianChatClient` | `chat()` / `streamChat()` | bailian-chat / bailian-stream-chat | LLM_PROVIDER | 百炼模型调用 |
| `OllamaChatClient` | `chat()` / `streamChat()` | ollama-chat / ollama-stream-chat | LLM_PROVIDER | Ollama 模型调用 |
| `SiliconFlowChatClient` | `chat()` / `streamChat()` | siliconflow-chat / siliconflow-stream-chat | LLM_PROVIDER | SiliconFlow 模型调用 |

### 嵌套节点示例

```
RagTraceRun (rag-stream-chat)
  ├── node: query-rewrite (REWRITE)
  ├── node: intent-resolve (INTENT)
  ├── node: retrieval-engine (RETRIEVE)
  │     └── node: multi-channel-retrieval (RETRIEVE_CHANNEL)
  ├── node: llm-stream-routing (LLM_ROUTING)
  │     └── node: bailian-stream-chat (LLM_PROVIDER)
  └── ...
```

---

## 七、查询与展示层

### 7.1 后端查询 API

**RagTraceController** 暴露 REST API：

| 接口 | 方法 | 说明 |
|------|------|------|
| `/rag/traces/runs` | GET | 分页查询链路运行记录 |
| `/rag/traces/runs/{traceId}` | GET | 查询链路详情 |
| `/rag/traces/runs/{traceId}/nodes` | GET | 查询链路节点列表 |

### 7.2 Dashboard 集成

`DashboardServiceImpl` 直接使用 `RagTraceRunMapper` 进行统计查询：

- `countTraceRuns()` -- 统计成功/失败次数
- `listDurations()` -- 获取耗时列表计算 P95
- `averageLatencyByDay/ByHour()` -- 按时间粒度统计平均延迟
- `countTraceRunsByDay/ByHour()` -- 按时间粒度统计成功/失败率

### 7.3 前端页面

前端在 `frontend/src/pages/admin/traces/` 下提供了完整的链路追踪管理页面：

- `RagTracePage.tsx` -- 链路列表页
- `RagTraceDetailPage.tsx` -- 链路详情页（含节点时间线）
- 组件包括 FilterBar（筛选）、RunsTable（表格）、StatCard（统计卡片）
- `traceUtils.ts` 提供时间线节点的偏移量和宽度百分比计算，用于可视化展示各节点的耗时分布

---

## 八、配置

**文件**: `bootstrap/src/main/resources/application.yaml`

```yaml
rag:
  trace:
    enabled: true          # 是否启用注解式 Trace 采集
    max-error-length: 1000 # 错误信息最大长度
```

---

## 九、关键设计总结

| 设计点 | 方案 | 优势 |
|--------|------|------|
| 两阶段写入 | INSERT RUNNING → UPDATE SUCCESS/ERROR | 即使异常也能记录部分数据 |
| TTL 跨线程透传 | TransmittableThreadLocal + TtlExecutors | 异步步骤也能正确关联链路 |
| 节点栈 | Deque<String> 维护父子关系 | 支持任意深度嵌套 |
| 根节点灵活 | @RagTraceRoot 预留 + 手动创建 | 适配 SSE 排队场景 |
| 错误截断 | maxErrorLength=1000 | 防止异常堆栈过大导致写入失败 |