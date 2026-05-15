# RAG 检索阶段详解

## 整体架构

```
StreamChatPipeline.retrieve()
    │
    └── RetrievalEngine.retrieve()
            │
            └── MultiChannelRetrievalEngine.retrieveKnowledgeChannels()
                    ├── 过滤启用的通道（按优先级排序）
                    ├── 并行执行所有通道 → List<SearchChannelResult>
                    └── 执行 PostProcessor 链 → List<RetrievedChunk>
```

## 一、通道选择与并行执行

**通道过滤器** (`isEnabled()`):

| 通道 | 优先级 | 启用条件 |
|------|--------|----------|
| IntentDirectedSearchChannel | 1 (最高) | 存在 KB 意图且置信度 >= minIntentScore (默认 0.5) |
| VectorGlobalSearchChannel | 10 (最低) | 无意图 或 所有意图置信度 < 阈值 |

**并行执行**：
- 使用 `ragRetrievalExecutor` 线程池并行执行所有启用的通道
- 每个通道独立完成向量检索，返回 `SearchChannelResult`

## 二、向量检索核心（以 Milvus 为例）

```java
MilvusRetrieverService.retrieve(query)
    │
    ├── EmbeddingService.embed(query)  →  List<Float> 向量
    │
    ├── 向量归一化 (L2 norm)
    │
    └── MilvusClient.search()
            - collectionName: 知识库对应的 collection
            - annsField: "embedding"
            - metricType: COSINE / IP
            - topK: 返回数量
            - outputFields: id, content, metadata
```

### 关键代码

```java
// MilvusRetrieverService.java
public List<RetrievedChunk> retrieve(RetrieveRequest retrieveParam) {
    // 1. 获取问题 embedding
    List<Float> emb = embeddingService.embed(retrieveParam.getQuery());
    float[] vec = toArray(emb);

    // 2. L2 归一化
    float[] norm = normalize(vec);

    // 3. 向量检索
    return retrieveByVector(norm, retrieveParam);
}
```

## 三、全局检索 vs 意图定向检索

### VectorGlobalSearchChannel（兜底策略）

```java
// 1. 获取所有 KB collection
List<String> collections = getAllKBCollections();

// 2. 并行在所有 collection 中检索
parallelRetriever.executeParallelRetrieval(question, collections, topK * multiplier);
```

- 当意图识别失败或置信度低时启用
- 在所有知识库中全局搜索
- 优先级最低，作为兜底策略

### IntentDirectedSearchChannel（精确策略）

```java
// 1. 提取 KB 意图
List<NodeScore> kbIntents = extractKbIntents(context);

// 2. 并行检索每个意图对应的知识库
parallelRetriever.executeParallelRetrieval(question, kbIntents, topK, multiplier);
```

- 基于意图识别结果定向检索
- 优先级最高，精确度最高

## 四、并行检索模板

`AbstractParallelRetriever` 定义了并行检索的模板方法：

```
executeParallelRetrieval(question, targets, topK)
    │
    ├── 1. 为每个 target 创建 CompletableFuture
    │     └── createRetrievalTask(question, target, topK)
    │
    ├── 2. 提交到线程池并行执行
    │
    ├── 3. 等待所有任务完成
    │
    └── 4. 合并结果返回 List<RetrievedChunk>
```

### 子类实现

| 类 | 模板方法实现 |
|---|---|
| CollectionParallelRetriever | targets = List\<String\> (collection 名称列表) |
| IntentParallelRetriever | targets = List\<NodeScore\> (意图列表) |

## 五、PostProcessor 链

### 1. DeduplicationPostProcessor (Order=1)

```java
// 按 chunk ID 去重
Map<String, RetrievedChunk> chunkMap = new LinkedHashMap<>();
for (result : sortedResults) {
    for (chunk : result.getChunks()) {
        if (!chunkMap.containsKey(key)) {
            chunkMap.put(key, chunk);
        } else if (chunk.getScore() > existing.getScore()) {
            chunkMap.put(key, chunk);  // 保留最高分
        }
    }
}
```

### 2. RerankPostProcessor (Order=10)

```java
// 调用 Rerank 模型重排序
return rerankService.rerank(
    context.getMainQuestion(),
    chunks,
    context.getTopK()
);
```

## 六、完整数据流

```
用户问题: "入职流程是什么？"
    │
    ▼
意图识别 → NodeScore(KB:hr_knowledge, 0.85)
    │
    ▼
IntentDirectedSearchChannel 启用（优先级 1）
    │
    ▼
MilvusClient.search(collection="hr_knowledge", topK=8)
    │
    ▼
返回: List<RetrievedChunk> (id, content, score)
    │
    ▼
DeduplicationPostProcessor → 按 ID 去重
    │
    ▼
RerankPostProcessor → Rerank 模型重排序
    │
    ▼
最终返回 topK 个最相关 Chunk
```

## 七、关键类一览

| 类 | 职责 |
|---|---|
| `MultiChannelRetrievalEngine` | 多通道检索编排器 |
| `IntentDirectedSearchChannel` | 意图定向检索通道 |
| `VectorGlobalSearchChannel` | 全局检索通道（兜底） |
| `AbstractParallelRetriever` | 并行检索模板类 |
| `CollectionParallelRetriever` | 按 collection 并行检索 |
| `IntentParallelRetriever` | 按意图并行检索 |
| `MilvusRetrieverService` | Milvus 向量检索实现 |
| `PgRetrieverService` | PostgreSQL 向量检索实现 |
| `DeduplicationPostProcessor` | 去重处理器 |
| `RerankPostProcessor` | 重排序处理器 |