# SSE 流式对话全局限流机制详解

## 一、限流架构总览

### 1.1 核心组件

| 组件 | 文件 | 职责 |
|------|------|------|
| `ChatQueueLimiter` | `bootstrap/src/main/java/com/nageoffer/ai/ragent/rag/service/ratelimit/ChatQueueLimiter.java` | 限流入口，协调排队逻辑与拒绝策略 |
| `FairDistributedRateLimiter` | `bootstrap/src/main/java/com/nageoffer/ai/ragent/rag/service/ratelimit/FairDistributedRateLimiter.java` | 分布式公平排队 + 许可抢占核心 |
| `chatEntryExecutor` | `bootstrap/src/main/java/com/nageoffer/ai/ragent/rag/config/ThreadPoolExecutorConfig.java` | 限流通过后的执行线程池 |
| `RAGRateLimitProperties` | `bootstrap/src/main/java/com/nageoffer/ai/ragent/rag/config/RAGRateLimitProperties.java` | 限流配置 |

### 1.2 整体调用链

```
前端 GET /rag/v3/chat
    │
    ▼
RAGChatController.chat()
    │
    ├── 创建 SseEmitter
    │
    └── RAGChatServiceImpl.streamChat()
            │
            └── ChatQueueLimiter.enqueue()
                    │
                    ├── 限流关闭? ── YES ──→ chatEntryExecutor.execute(onAcquire)
                    │                           │
                    │                           └── 执行业务逻辑
                    │
                    └── 限流开启
                            │
                            └── FairDistributedRateLimiter.acquire()
                                    │
                                    ├── 写入 entry 存活标记（Redis Bucket）
                                    ├── 入队（Redis ZSet）
                                    ├── 立即尝试获取许可
                                    │       ├── 成功 → 提交 chatEntryExecutor 执行
                                    │       └── 失败 → 定时轮询 + 事件驱动等待
                                    │
                                    └── 排队中...
                                            │
                                            ├── 超时 → onTimeout → 发送 REJECT 事件
                                            ├── 取消（SSE断开）→ cleanup → 释放资源
                                            └── 获取许可 → 执行业务逻辑
```

---

## 二、使用的数据结构详解

### 2.1 Redis 数据结构清单

| 数据结构 | Redis Key | 作用 | 使用时机 |
|----------|-----------|------|----------|
| `RPermitExpirableSemaphore` | `rag:global:chat:semaphore` | 并发槽位控制（信号量） | 1. 初始化时设置 permits<br>2. 抢占时 tryAcquire<br>3. 释放时 release |
| `RScoredSortedSet` (ZSet) | `rag:global:chat:queue` | 公平排队队列 | 1. 请求入队时 ZADD<br>2. Lua 判断时 ZRANGE<br>3. 出队时 ZREM |
| `RBucket` | `rag:global:chat:entry:{requestId}` | 请求存活标记（带 TTL） | 1. 入队前 SET（带 TTL）<br>2. Lua 判断时 EXISTS<br>3. 清理时 DEL |
| `RTopic` | `rag:global:chat:queue:notify` | 许可释放通知 | 1. 释放 permit 后 PUBLISH<br>2. 启动时 SUBSCRIBE |
| `RAtomicLong` | `rag:global:chat:queue:seq` | 队列序号生成器 | 入队时 INCREMENT 生成 score |

---

## 三、数据结构的使用时机与原因

### 3.1 RPermitExpirableSemaphore（信号量）

**使用时机**：

1. **初始化**：`start()` 方法中
   ```java
   redissonClient.getPermitExpirableSemaphore(semaphoreKey)
       .trySetPermits(maxPermitsSupplier.getAsInt());
   ```

2. **获取许可**：`tryAcquireIfReady()` 中
   ```java
   String permitId = tryAcquirePermit();
   ```

3. **释放许可**：业务逻辑执行完毕或清理时
   ```java
   releasePermitQuietly(permitId);
   ```

**为什么用信号量？**

| 特性 | 说明 |
|------|------|
| **并发控制** | 精确控制同时执行的请求数量（max-concurrent） |
| **可过期** | `leaseSeconds` 参数，防止 JVM 崩溃导致 permit 永久泄漏 |
| **精确释放** | 每个 permit 有唯一 ID，必须用该 ID 释放，避免误释放 |
| **分布式** | 基于 Redis，多实例部署下全局一致 |

**替代方案对比**：

| 方案 | 问题 |
|------|------|
| Java 原生 `Semaphore` | 仅单进程有效，分布式场景失效 |
| 计数 + CAS | 无法处理 permit 过期释放、精确释放等复杂场景 |

---

### 3.2 RScoredSortedSet（ZSet）

**使用时机**：

1. **入队**：`acquire()` 中
   ```java
   queue.add(nextQueueSeq(), ticket.requestId);
   ```

2. **Lua 脚本判断**：`queue_claim_atomic.lua`
   ```lua
   local headEntries = redis.call('ZRANGE', queueKey, 0, maxRank + slack - 1)
   redis.call('ZREM', queueKey, requestId)
   ```

3. **重入队**：`tryAcquireIfReady()` 中（拿不到 permit 时）
   ```java
   queue.add(claimedScore, ticket.requestId);  // 用原来的 score 保持位置
   ```

4. **清理**：`Ticket.cleanup()` 中
   ```java
   redissonClient.getScoredSortedSet(queueKey).remove(requestId);
   ```

**为什么用 ZSet？**

| 特性 | 说明 |
|------|------|
| **有序性** | 按 score 排序，保证 FIFO 公平性 |
| **范围查询** | `ZRANGE(0, avail-1)` 快速获取队头窗口 |
| **原子操作** | 配合 Lua 脚本实现原子性判断 |

**score 的设计**：

```java
private long nextQueueSeq() {
    RAtomicLong seq = redissonClient.getAtomicLong(queueSeqKey);
    return seq.incrementAndGet();  // 全局自增序号
}
```

| 方案 | 问题 |
|------|------|
| `System.currentTimeMillis()` | 同一毫秒重复，不够精确 |
| `nanoTime` | 多机器不同步，分布式失效 |
| **Redis 原子自增** | 全局唯一、严格递增、分布式一致 |

---

### 3.3 RBucket（存活标记）

**使用时机**：

1. **入队前设置**：`acquire()` 中
   ```java
   setEntryMarker(ticket.requestId, req.maxWaitMillis());
   // 内部: bucket.set("1", Duration.ofMillis(ttlMillis))
   ```

2. **Lua 脚本检查**：`queue_claim_atomic.lua`
   ```lua
   if redis.call('EXISTS', entryPrefix .. member) == 1 then
       -- 存活，计数
   else
       -- 僵尸，ZREM 清理
   end
   ```

3. **清理时删除**：
   - Lua 成功出队时：`DEL entryKey`
   - Ticket.cleanup() 时：`deleteEntryMarker(requestId)`

**为什么用存活标记？**

解决 **僵尸请求** 问题：

```
场景：
1. 请求 A 入队 ZSet，score=1001
2. JVM 崩溃或请求被取消，但 ZSet 条目仍存在
3. 后续请求 B、C、D 入队
4. 队头检查时，A 永远排在第一位，但实际已失效
5. → 队头窗口被僵尸阻塞！
```

**解决方案**：

```
每个请求入队时：
  1. 先 SET entry:{requestId}，TTL = maxWait + 5s 缓冲
  2. 再 ZADD 到队列

Lua 检查时：
  1. ZRANGE 取队头窗口
  2. 对每个成员 EXISTS entry:{member}
  3. 不存在 → ZREM 清理（僵尸）
  4. 存在 → 计入存活数
```

**TTL 设计**：
```java
private static final long ENTRY_TTL_BUFFER_MILLIS = 5_000L;  // 5秒缓冲

long ttlMillis = Math.max(remainingMillis, 1L) + ENTRY_TTL_BUFFER_MILLIS;
```

- 缓冲 5 秒，避免毫秒级时钟漂移导致误判
- JVM 崩溃后，key 自然过期，下次 Lua 检查自动清理

---

### 3.4 RTopic（发布订阅）

**使用时机**：

1. **启动时订阅**：`start()` 中
   ```java
   RTopic topic = redissonClient.getTopic(notifyTopicKey);
   notifyListenerId = topic.addListener(String.class, (channel, msg) -> pollNotifier.fire());
   ```

2. **释放 permit 时发布**：
   ```java
   private void publishQueueNotify() {
       redissonClient.getTopic(notifyTopicKey).publish("permit_changed");
   }
   ```

**为什么用 Topic？**

| 问题 | 解决方案 |
|------|----------|
| 纯轮询（200ms 间隔） | 响应延迟大，浪费资源 |
| 纯事件驱动 | 通知丢失时永远等待 |
| **轮询 + 事件驱动** | 轮询保底，事件驱动加速响应 |

**PollNotifier 机制**：

```java
private static final class PollNotifier {
    private final AtomicBoolean firing = new AtomicBoolean(false);
    private final AtomicInteger pendingNotifications = new AtomicInteger(0);

    void fire() {
        pendingNotifications.incrementAndGet();
        if (!firing.compareAndSet(false, true)) {
            return;  // 合并连续通知，避免风暴
        }
        executor.execute(() -> {
            do {
                pendingNotifications.set(0);
                // 扫描所有 poller...
            } while (pendingNotifications.get() > 0 && firing.compareAndSet(false, true));
        });
    }
}
```

- 连续到达的通知只触发一次扫描（合并）
- 扫描前检查 permit 是否真的可用，避免无效扫描

---

## 四、Tomcat 线程释放机制

### 4.1 会不会阻塞 Tomcat 线程？

**答案：不会。无论是否拿到许可，都不会阻塞 Tomcat 线程。**

### 4.2 两种场景对比

| 场景 | Tomcat 线程状态 | 原因 |
|------|----------------|------|
| **拿到许可** | 立即释放 | `acquire()` 返回 → `streamChat()` 返回 → Controller return emitter |
| **没拿到许可（排队）** | 立即释放 | `scheduleQueuePoll()` 只是注册任务，**不等待** → 立即返回 |

### 4.3 调用链分析

```java
// RAGChatController.chat()  ← Tomcat 线程执行
@GetMapping(value = "/rag/v3/chat", produces = "text/event-stream;charset=UTF-8")
public SseEmitter chat(...) {
    SseEmitter emitter = new SseEmitter(...);
    ragChatService.streamChat(question, conversationId, deepThinking, emitter);
    return emitter;  // ← Tomcat 线程在这里返回！
}
```

```java
// RAGChatServiceImpl.streamChat()
@Override
public void streamChat(..., SseEmitter emitter) {
    StreamCallback callback = callbackFactory.createChatEventHandler(emitter, ...);

    chatQueueLimiter.enqueue(question, actualConversationId, emitter,
            () -> {
                // 这是 onAcquire 回调，不是现在执行！
                // 只有拿到 permit 后才会在 chatEntryExecutor 中执行
                traceRunner.run(...);
            });
    
    // ← 这里立即返回，不等待！
}
```

```java
// ChatQueueLimiter.enqueue()
public void enqueue(..., Runnable onAcquire) {
    if (!rateLimitProperties.getGlobalEnabled()) {
        // 限流关闭：提交到线程池，立即返回
        chatEntryExecutor.execute(onAcquire);
        return;
    }

    // 限流开启：构建请求，提交到限流器
    chatRateLimiter.acquire(AcquireRequest.builder()
            .onAcquired(onAcquire)  // 回调，不是立即执行
            .onTimeout(...)          // 超时回调
            .onAcquiredExecutor(chatEntryExecutor)  // 指定执行线程池
            .build());
    
    // ← 这里立即返回！
}
```

### 4.3 Tomcat 线程释放时机

| 阶段 | Tomcat 线程状态 |
|------|-----------------|
| 进入 `Controller.chat()` | 占用 |
| 调用 `streamChat()` | 占用 |
| 调用 `enqueue()` | 占用 |
| 调用 `acquire()` → 写入 Redis → 入队 | 占用 |
| `acquire()` 返回 | **释放** |
| `streamChat()` 返回 | 已释放 |
| `Controller.chat()` return emitter | 已释放 |

### 4.4 关键：SseEmitter 的异步特性

`SseEmitter` 返回后，Tomcat 线程释放，但 HTTP 连接保持：

```
Tomcat 线程池          HTTP 连接
    │                     │
    ├── 线程 1 处理请求    │ 保持打开
    ├── 创建 emitter      │
    ├── return emitter    │
    ├── 线程 1 释放        │ 仍保持打开（等待 SSE 事件）
    │                     │
    ├── 线程池异步线程     │
    │   执行业务逻辑       │
    │   发送 SSE 事件      │ 事件通过连接推送
    │   ...               │
    │   emitter.complete() │ 连接关闭
    └──                   └──
```

**Spring MVC 对 SseEmitter 的处理**：

1. Controller 返回 `SseEmitter`
2. Spring 将其包装为 `ResponseBodyEmitterReturnValueHandler`
3. Tomcat 线程释放，但 `AsyncContext` 保持连接
4. 后续通过 `emitter.send()` / `emitter.complete()` 操作连接

### 4.7 排队等待期间的线程状态

```
排队期间：
┌─────────────────────────────────────────────────┐
│ Tomcat 线程     │ 已释放（回到线程池）            │
│ HTTP 连接       │ 保持打开（AsyncContext）        │
│ 业务执行线程    │ 未启动（等待 permit）            │
│ 轮询线程        │ scheduler 定时检查（200ms 间隔） │
│ Redis 状态      │ requestId 在 ZSet 中等待        │
└─────────────────────────────────────────────────┘
```

---

## 五、完整限流流程时序图

```
用户请求 (Tomcat 线程)
    │
    ├─── 创建 SseEmitter
    │
    ├─── streamChat()
    │       │
    │       └─── enqueue(onAcquire=业务逻辑)
    │               │
    │               └─── acquire(AcquireRequest)
    │                       │
    │                       ├─── Ticket ticket = new Ticket()
    │                       │        └── requestId = 雪花ID
    │                       │
    │                       ├─── setEntryMarker(requestId, TTL)
    │                       │        └── Redis: SET entry:{id} EX 25s
    │                       │
    │                       ├─── queue.add(seq++, requestId)
    │                       │        └── Redis: ZADD queue 1001 "{id}"
    │                       │
    │                       ├─── tryAcquireIfReady()
    │                       │        ├── avail = availablePermits()
    │                       │        ├── Lua 判断是否在队头
    │                       │        └── 尝试 acquire permit
    │                       │
    │                       ├─── 成功?
    │                       │       ├── YES → submit to chatEntryExecutor
    │                       │       └── NO  → scheduleQueuePoll()
    │                       │
    │                       └─── return void  ← Tomcat 线程释放！
    │
    └─── return emitter  ← Tomcat 线程回到线程池！


排队等待中 (Scheduler 线程)
    │
    ├─── 定时轮询 (200ms 间隔)
    │        └── tryAcquireIfReady()
    │
    ├─── 或 收到 Topic 通知
    │        └── PollNotifier.fire() → 立即检查
    │
    └─── 重复直到：
            ├── 超时 → onTimeout → 发送 REJECT/DONE
            ├── SSE 断开 → cancel → cleanup
            └── 拿到 permit → grant()


获取许可 (chatEntryExecutor 线程)
    │
    ├─── ticket.grant(permitId)
    │        └── CAS: PENDING → GRANTED
    │
    ├─── onAcquired.run()  ← 业务逻辑开始
    │        └── traceRunner.run()
    │                └── StreamChatPipeline.execute()
    │                        ├── loadMemory()
    │                        ├── rewriteQuery()
    │                        ├── ...
    │                        └── streamRagResponse()
    │                                └── emitter.send() 发送 SSE 事件
    │
    └─── finally:
            ├── releasePermitQuietly(permitId)
            │        └── Redis: semaphore.release(permitId)
            │
            └── publishQueueNotify()
                     └── Redis: PUBLISH notify "permit_changed"
                             → 唤醒其他等待者
```

---

## 六、配置参数说明

| 参数 | 默认值 | 说明 | 影响 |
|------|--------|------|------|
| `global.enabled` | `true` | 是否启用限流 | `false` 时跳过限流逻辑 |
| `global.max-concurrent` | `50` | 最大并发数 | 信号量 permits 数量 |
| `global.max-wait-seconds` | `20` | 排队超时时间 | 超过后拒绝，发送 REJECT 事件 |
| `global.lease-seconds` | `600` | 许可自动过期时间 | JVM 崩溃后的兜底释放 |
| `global.poll-interval-ms` | `200` | 轮询间隔 | 事件驱动失效时的保底频率 |

---

## 七、核心设计亮点

### 7.1 公平性保证

1. **ZSet + 自增 score**：严格 FIFO
2. **Lua 原子判断**：只有队头窗口内的请求才能抢占
3. **重入队保持原位**：拿不到 permit 时用原始 score 重入

### 7.2 防泄漏机制

1. **信号量可过期**：`leaseSeconds` 兜底
2. **try/finally 释放**：业务逻辑包裹
3. **SSE 生命周期绑定**：`emitter.onCompletion/onTimeout/onError` → cancel
4. **entry TTL 自动过期**：JVM 崩溃后自动清理

### 7.3 高性能

1. **事件驱动 + 轮询保底**：响应快、资源省
2. **通知合并**：连续通知只扫描一次
3. **快速失败检查**：`availablePermits <= 0` 直接返回
4. **Redis 操作最小化**：Lua 脚本一次往返完成多操作

### 7.4 优雅降级

1. **拒绝时仍记录会话**：用户可以看到拒绝消息
2. **发送完整 SSE 事件**：`META` → `REJECT` → `FINISH` → `DONE`
3. **限流开关**：可动态关闭限流

---

## 八、关键代码位置速查

| 功能 | 文件 | 方法/行号 |
|------|------|-----------|
| 限流入口 | `ChatQueueLimiter.java` | `enqueue()` (L63) |
| 核心限流器 | `FairDistributedRateLimiter.java` | `acquire()` (L138) |
| 抢占逻辑 | `FairDistributedRateLimiter.java` | `tryAcquireIfReady()` (L301) |
| Lua 脚本 | `resources/lua/queue_claim_atomic.lua` | 全文 |
| 线程池配置 | `ThreadPoolExecutorConfig.java` | `chatEntryExecutor()` (L179) |
| 配置类 | `RAGRateLimitProperties.java` | 全文 |
| SSE Controller | `RAGChatController.java` | `chat()` (L50) |
