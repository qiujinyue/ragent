# 对话记忆 Redis 缓存优化方案

## 概述

当前对话记忆模块采用 JDBC 直读模式，每次加载对话历史都需要查询数据库，在高并发场景下性能成为瓶颈。本方案引入 Redis 作为缓存层，通过合理的数据结构设计和缓存策略，大幅提升记忆模块的读取性能。

### 优化目标

| 指标 | 优化前 | 优化后 | 预期提升 |
|------|--------|--------|----------|
| 历史加载延迟 | ~50-100ms (DB查询) | ~1-5ms (Redis) | 10-50倍 |
| 摘要加载延迟 | ~30-50ms (DB查询) | ~1-3ms (Redis) | 10-30倍 |
| 数据库查询QPS | 高 | 显著降低 | - |

---

## 一、Redis 数据结构设计

### 1.1 整体架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          缓存层 (Redis)                                │
│  ┌─────────────────────────────┐  ┌─────────────────────────────────┐  │
│  │      压缩记忆缓存           │  │        对话历史缓存              │  │
│  │  Hash: summary:{uid}:{cid}  │  │  List: history:{uid}:{cid}     │  │
│  │  ├─ content                 │  │  [msg1, msg2, msg3, ...]       │  │
│  │  ├─ lastMessageId           │  │                                 │  │
│  │  ├─ updateTime              │  └─────────────────────────────────┘  │
│  │  └─ version                 │                                      │
│  └─────────────────────────────┘                                      │
└─────────────────────────────────────┬──────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         持久层 (PostgreSQL)                           │
│  conversation_summary      │  conversation_message                     │
└─────────────────────────────────────────────────────────────────────────┘
```

### 1.2 压缩记忆缓存（摘要）

**数据结构：Redis Hash**

| Key 格式 | 说明 |
|----------|------|
| `ragent:memory:summary:{userId}:{conversationId}` | 存储对话摘要信息 |

**Hash 字段定义：**

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| `content` | String | 摘要内容（最大200字符） | `"Ragent是RAG系统..."` |
| `lastMessageId` | String | 摘要覆盖的最后消息ID | `"12345"` |
| `updateTime` | Long | 最后更新时间戳（毫秒） | `1699999999999` |
| `version` | Long | 版本号（乐观锁） | `1` |

**设计理由：**
- Hash 支持结构化存储，便于单独更新某个字段
- version 字段支持乐观锁，防止并发更新冲突
- 字段级读取，只获取需要的数据

### 1.3 对话历史缓存

**数据结构：Redis List**

| Key 格式 | 说明 |
|----------|------|
| `ragent:memory:history:{userId}:{conversationId}` | 存储对话历史消息列表 |

**List 元素格式（JSON 序列化）：**

```json
{
  "id": "msg_abc123",
  "role": "user",
  "content": "你好，我想了解Ragent项目",
  "timestamp": 1699999999999
}
```

**设计理由：**
- List 保证消息的时间顺序
- `rightPush` 追加操作 O(1) 复杂度
- `range` 查询最近 N 条消息高效
- `trim` 自动裁剪，控制列表长度

### 1.4 缓存键命名规范

| 类型 | 前缀 | 完整格式 | 示例 |
|------|------|----------|------|
| 摘要 | `ragent:memory:summary:` | `ragent:memory:summary:{userId}:{conversationId}` | `ragent:memory:summary:user_123:conv_456` |
| 历史 | `ragent:memory:history:` | `ragent:memory:history:{userId}:{conversationId}` | `ragent:memory:history:user_123:conv_456` |

---

## 二、缓存策略

### 2.1 读写模式：Read-Through + Write-Behind

**读取流程：**
```
用户请求 → 检查 Redis 缓存 → 命中 → 返回
                          ↓ 未命中
                    查询数据库 → 更新缓存 → 返回
```

**写入流程：**
```
写入数据库 → 返回响应 → 异步更新 Redis 缓存
                     ↓ (@Async 或消息队列)
                更新缓存内容
```

### 2.2 TTL 策略

| 缓存类型 | TTL | 刷新时机 |
|----------|-----|----------|
| 摘要缓存 | 30分钟 | 每次读取时刷新 |
| 历史缓存 | 30分钟 | 每次写入/读取时刷新 |

**代码示例：**
```java
// 读取时刷新过期时间
redisTemplate.expire(key, 30, TimeUnit.MINUTES);
```

### 2.3 缓存失效策略

| 场景 | 策略 | 说明 |
|------|------|------|
| 消息追加 | 同步更新缓存 | 保证缓存一致性 |
| 摘要更新 | 同步更新缓存 | 保证缓存一致性 |
| 会话删除 | 删除相关缓存 | 清理无效数据 |
| 缓存过期 | TTL 自动失效 | 自动清理 |
| 手动刷新 | 删除缓存 | 下次读取重新加载 |

---

## 三、核心代码实现

### 3.1 Redis 配置类

```java
@Configuration
public class RedisMemoryCacheConfig {

    @Value("${rag.memory.cache.ttl-minutes:30}")
    private int cacheTtlMinutes;

    @Value("${rag.memory.cache.max-history-size:8}")
    private int maxHistorySize;

    @Bean
    public RedisTemplate<String, Object> memoryRedisTemplate(RedisConnectionFactory factory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(factory);
        
        // JSON 序列化器
        Jackson2JsonRedisSerializer<Object> serializer = 
            new Jackson2JsonRedisSerializer<>(Object.class);
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        serializer.setObjectMapper(mapper);
        
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(serializer);
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(serializer);
        
        template.afterPropertiesSet();
        return template;
    }
}
```

### 3.2 缓存层服务类

```java
@Service
public class RedisMemoryCacheService {

    private static final String SUMMARY_KEY_PREFIX = "ragent:memory:summary:";
    private static final String HISTORY_KEY_PREFIX = "ragent:memory:history:";
    
    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;
    private final int cacheTtlMinutes;
    private final int maxHistorySize;

    /**
     * 获取摘要缓存
     */
    public Optional<Map<String, Object>> getSummary(String userId, String conversationId) {
        String key = buildSummaryKey(userId, conversationId);
        Map<Object, Object> hash = redisTemplate.opsForHash().entries(key);
        
        if (hash == null || hash.isEmpty()) {
            return Optional.empty();
        }
        
        // 刷新过期时间
        redisTemplate.expire(key, cacheTtlMinutes, TimeUnit.MINUTES);
        
        Map<String, Object> result = new HashMap<>();
        hash.forEach((k, v) -> result.put(String.valueOf(k), v));
        return Optional.of(result);
    }

    /**
     * 设置摘要缓存
     */
    public void setSummary(String userId, String conversationId, 
                          String content, String lastMessageId) {
        String key = buildSummaryKey(userId, conversationId);
        Map<String, Object> hash = new HashMap<>();
        hash.put("content", content);
        hash.put("lastMessageId", lastMessageId);
        hash.put("updateTime", System.currentTimeMillis());
        hash.put("version", System.currentTimeMillis()); // 简化版本号
        
        redisTemplate.opsForHash().putAll(key, hash);
        redisTemplate.expire(key, cacheTtlMinutes, TimeUnit.MINUTES);
    }

    /**
     * 获取历史缓存
     */
    public Optional<List<ChatMessage>> getHistory(String userId, String conversationId) {
        String key = buildHistoryKey(userId, conversationId);
        List<Object> cached = redisTemplate.opsForList().range(key, -maxHistorySize, -1);
        
        if (cached == null || cached.isEmpty()) {
            return Optional.empty();
        }
        
        // 刷新过期时间
        redisTemplate.expire(key, cacheTtlMinutes, TimeUnit.MINUTES);
        
        List<ChatMessage> messages = cached.stream()
                .map(obj -> deserialize((String) obj))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        
        return Optional.of(messages);
    }

    /**
     * 追加消息到历史缓存
     */
    public void appendHistory(String userId, String conversationId, ChatMessage message) {
        String key = buildHistoryKey(userId, conversationId);
        
        // 追加到列表尾部
        redisTemplate.opsForList().rightPush(key, serialize(message));
        
        // 裁剪到最大长度
        redisTemplate.opsForList().trim(key, -maxHistorySize, -1);
        
        // 设置过期时间
        redisTemplate.expire(key, cacheTtlMinutes, TimeUnit.MINUTES);
    }

    /**
     * 删除会话缓存
     */
    public void deleteSessionCache(String userId, String conversationId) {
        String summaryKey = buildSummaryKey(userId, conversationId);
        String historyKey = buildHistoryKey(userId, conversationId);
        
        redisTemplate.delete(summaryKey);
        redisTemplate.delete(historyKey);
    }

    private String buildSummaryKey(String userId, String conversationId) {
        return SUMMARY_KEY_PREFIX + userId + ":" + conversationId;
    }

    private String buildHistoryKey(String userId, String conversationId) {
        return HISTORY_KEY_PREFIX + userId + ":" + conversationId;
    }

    private String serialize(ChatMessage message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            log.error("序列化消息失败", e);
            return null;
        }
    }

    private ChatMessage deserialize(String json) {
        try {
            return objectMapper.readValue(json, ChatMessage.class);
        } catch (JsonProcessingException e) {
            log.error("反序列化消息失败", e);
            return null;
        }
    }
}
```

### 3.3 缓存增强的 MemoryStore

```java
@Service
public class CachedConversationMemoryStore implements ConversationMemoryStore {

    private final ConversationMemoryStore delegate;  // 原始 JDBC 实现
    private final RedisMemoryCacheService cacheService;

    @Override
    public List<ChatMessage> loadHistory(String conversationId, String userId) {
        // 1. 尝试从缓存读取
        Optional<List<ChatMessage>> cached = cacheService.getHistory(userId, conversationId);
        if (cached.isPresent()) {
            log.debug("缓存命中 - conversationId: {}", conversationId);
            return cached.get();
        }

        // 2. 缓存未命中，从数据库读取
        log.debug("缓存未命中，从数据库加载 - conversationId: {}", conversationId);
        List<ChatMessage> history = delegate.loadHistory(conversationId, userId);

        // 3. 异步更新缓存
        updateCacheAsync(userId, conversationId, history);

        return history;
    }

    @Override
    public String append(String conversationId, String userId, ChatMessage message) {
        // 1. 先写入数据库
        String messageId = delegate.append(conversationId, userId, message);

        // 2. 同步更新缓存
        cacheService.appendHistory(userId, conversationId, message);

        return messageId;
    }

    @Override
    public void refreshCache(String conversationId, String userId) {
        // 删除缓存，下次读取时重新加载
        cacheService.deleteSessionCache(userId, conversationId);
    }

    @Async("memoryCacheExecutor")
    public void updateCacheAsync(String userId, String conversationId, List<ChatMessage> history) {
        try {
            // 逐个追加到缓存
            for (ChatMessage message : history) {
                cacheService.appendHistory(userId, conversationId, message);
            }
            log.debug("异步更新缓存完成 - conversationId: {}", conversationId);
        } catch (Exception e) {
            log.error("异步更新缓存失败 - conversationId: {}", conversationId, e);
        }
    }
}
```

### 3.4 缓存增强的 SummaryService

```java
@Service
public class CachedConversationMemorySummaryService 
        implements ConversationMemorySummaryService {

    private final ConversationMemorySummaryService delegate;
    private final RedisMemoryCacheService cacheService;

    @Override
    public ChatMessage loadLatestSummary(String conversationId, String userId) {
        // 1. 尝试从缓存读取
        Optional<Map<String, Object>> cached = cacheService.getSummary(userId, conversationId);
        if (cached.isPresent()) {
            Map<String, Object> summaryMap = cached.get();
            String content = (String) summaryMap.get("content");
            if (StringUtils.isNotBlank(content)) {
                log.debug("摘要缓存命中 - conversationId: {}", conversationId);
                return ChatMessage.system(content);
            }
        }

        // 2. 缓存未命中，从数据库读取
        log.debug("摘要缓存未命中 - conversationId: {}", conversationId);
        ChatMessage summary = delegate.loadLatestSummary(conversationId, userId);

        // 3. 异步更新缓存
        if (summary != null) {
            updateSummaryCacheAsync(userId, conversationId, summary);
        }

        return summary;
    }

    @Override
    public void compressIfNeeded(String conversationId, String userId, ChatMessage message) {
        // 先调用原始实现
        delegate.compressIfNeeded(conversationId, userId, message);
        
        // 压缩完成后异步刷新缓存（实际实现中需要在压缩完成后触发）
    }

    @Override
    public ChatMessage decorateIfNeeded(ChatMessage summary) {
        return delegate.decorateIfNeeded(summary);
    }

    @Async("memoryCacheExecutor")
    public void updateSummaryCacheAsync(String userId, String conversationId, ChatMessage summary) {
        try {
            cacheService.setSummary(userId, conversationId, 
                    summary.getContent(), null);
            log.debug("异步更新摘要缓存完成 - conversationId: {}", conversationId);
        } catch (Exception e) {
            log.error("异步更新摘要缓存失败 - conversationId: {}", conversationId, e);
        }
    }
}
```

---

## 四、线程池配置

```java
@Configuration
public class MemoryCacheExecutorConfig {

    @Bean(name = "memoryCacheExecutor")
    public Executor memoryCacheExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("memory-cache-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }
}
```

---

## 五、性能对比

### 5.1 理论分析

| 操作 | JDBC 直读 | Redis 缓存 | 提升倍数 |
|------|-----------|------------|----------|
| 加载历史（8条消息） | ~50ms | ~2ms | 25x |
| 加载摘要 | ~30ms | ~1ms | 30x |
| 追加消息 | ~20ms | ~0.5ms | 40x |
| 数据库QPS | 高 | 低（缓存命中率高时） | - |

### 5.2 缓存命中率估算

假设用户平均会话时长为 10 轮对话：

| 指标 | 数值 |
|------|------|
| 首次加载（缓存未命中） | 1次 |
| 后续加载（缓存命中） | 9次 |
| 理论命中率 | 90% |

---

## 六、配置与部署

### 6.1 application.yaml 配置

```yaml
rag:
  memory:
    cache:
      enabled: true                    # 启用缓存
      ttl-minutes: 30                  # 缓存过期时间（分钟）
      max-history-size: 8              # 最大历史消息数（4轮 = 8条）

spring:
  data:
    redis:
      host: localhost
      port: 6379
      password: your_password
      timeout: 5000ms
      lettuce:
        pool:
          max-active: 20
          max-idle: 10
          min-idle: 5
```

### 6.2 部署注意事项

1. **Redis 高可用**：生产环境建议使用 Redis Cluster 或 Sentinel 模式
2. **内存配置**：根据预估的缓存数据量配置 Redis 内存上限
3. **监控告警**：监控 Redis 命中率、内存使用、连接数等指标
4. **备份策略**：定期备份 Redis 数据，防止数据丢失

---

## 七、风险与应对

| 风险 | 应对策略 |
|------|----------|
| 缓存与数据库不一致 | 短 TTL + 写入时同步更新缓存 |
| Redis 故障 | 降级到 JDBC 直读模式 |
| 缓存击穿 | 使用互斥锁防止大量请求穿透到数据库 |
| 缓存雪崩 | 设置随机 TTL，避免大量缓存同时过期 |
| 内存溢出 | 设置 Redis 内存上限和淘汰策略（LRU） |

---

## 八、总结

### 方案优势

1. **性能提升**：读取延迟从数十毫秒降至毫秒级
2. **降低数据库压力**：高命中率下大幅减少数据库查询
3. **异步更新**：写入操作不阻塞主流程
4. **优雅降级**：Redis 故障时自动降级到 JDBC
5. **易于扩展**：支持水平扩展 Redis Cluster

### 实施步骤

1. 引入 Redis 依赖和配置
2. 实现 RedisMemoryCacheService
3. 创建 CachedConversationMemoryStore
4. 创建 CachedConversationMemorySummaryService
5. 配置异步线程池
6. 测试验证性能和一致性
7. 灰度发布，逐步上线