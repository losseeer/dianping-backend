# 快评后端设计文档（Java Spring Boot 后端）

> 本文档面向 Java 后端开发者与面试者，系统化梳理「快评」交易平台后端的工程分层、关键技术实现、数据库设计、API 协议、高可用策略与配置管理。文档与代码状态对齐于 2026-08-11。
>
> 面向读者：
> - **开发者**：快速定位每个模块的入口文件、关键技术决策、配置项与启动流程
> - **面试者**：从中抽取「秒杀 / 缓存三防 / ES 同义词 / 熔断限流 / 双层拦截器 / 死信延迟」等 T0 八股的工程化答辩要点
> - **测试**：通过 §6 API 端点表 + §8 防御矩阵构造针对 P0/P1/P2 的回归用例

---

## 一、系统总体架构

### 1.1 后端定位

Java Spring Boot 后端（:8081）是「快评」平台的**交易与业务核心**，承担：

1. **业务主链路**：用户登录、商户查询、优惠券秒杀、订单管理、支付退款、社交互动（探店 / 关注 / 点赞 / 评论）、个性化推荐
2. **数据与缓存层**：MySQL 持久化 + Redis 多结构缓存 + Elasticsearch 全文搜索
3. **异步与解耦**：RabbitMQ 异步下单、支付通知、退款处理、订单超时自动取消
4. **接口防护**：AOP 限流 + 熔断降级 + 双层鉴权拦截器
5. **Agent 微服务支撑**：为两个 Python Agent 微服务提供 HTTP 数据接口、Redis 共享缓存、ES 同义词检索能力

### 1.2 系统部署拓扑

```
┌──────────────────────────────────────────────────────────────────────┐
│                       前端 (Vue 3 + TS + Vite + Pinia)                │
│                       · 路由守卫拦截 /agent 未登录                     │
│                       · AgentChat.vue / Search.vue / Seckill.vue      │
└──────────────────────────────┬───────────────────────────────────────┘
                               │ HTTPS / Axios 封装（统一错误提示）
┌──────────────────────────────▼───────────────────────────────────────┐
│                Java Spring Boot :8081（交易 + 业务 + 搜索）            │
│  ┌─────────────┐ ┌────────────┐ ┌────────────┐ ┌──────────────────┐ │
│  │ Interceptor  │ │ Controller │ │  Service   │ │   Listener (MQ)  │ │
│  │ · 双层鉴权   │ │ 13 个模块  │ │ 14 个 Impl │ │ 秒杀/支付/延迟    │ │
│  │ · 限流熔断   │ │            │ │            │ │                  │ │
│  └─────────────┘ └────────────┘ └────────────┘ └──────────────────┘ │
│  ┌─────────────┐ ┌────────────┐ ┌────────────┐ ┌──────────────────┐ │
│  │ AOP 切面     │ │ Redisson   │ │ MyBatisPlus│ │ Elasticsearch7   │ │
│  │·@RateLimit   │ │ 分布式锁   │ │   ORM      │ │·synonym_graph    │ │
│  │  令牌桶      │ │ WatchDog   │ │  分页      │ │·IK双analyzer     │ │
│  │·@CircuitBreaker三态熔断器    │ │            │ │·search_analyzer  │ │
│  │  fallback→MySQL             │ │            │ │  synonyms 扩展    │ │
│  └─────────────┘ └────────────┘ └────────────┘ └──────────────────┘ │
└──────┬────────┬──────────┬────────────┬────────────────────────────┘
       │        │          │            │
┌──────▼───┐ ┌──▼────┐ ┌───▼─────┐ ┌───▼──────────┐
│  MySQL    │ │ Redis │ │RabbitMQ │ │Elasticsearch │
│  :3306    │ │:6379  │ │ :5672    │ │   :9200       │
│  8 张核心 │ │ 共享  │ │ 3 个DLX │ │ IK + synonyms│
│ +支付+搜索│ │        │ │          │ │  rebuild 接口│
└────┬─────┘ └──┬─────┘ └─────────┘ └──────────────┘
     │          │ 共享（Token / 缓存 / 锁 / 轨迹 / 记忆）
┌─────▼──────────▼──────────────────────────────────────────────────────┐
│           Python Agent 微服务 (FastAPI, 独立部署 可拆机器)              │
│  ┌────────────────────────────────────┐  ┌────────────────────────────┐ │
│  │  Agent1 :8001  评价摘要 Pipeline   │  │  Agent2 :8002  智能推荐    │ │
│  │  · 两阶段 LLM: 逐条分析 + 汇总     │──│  · LangGraph ReAct 7 节点  │ │
│  │  · 30min Redis 缓存               │  │  · 9 维偏好 + Playbook ACE │ │
│  └────────────────────────────────────┘  │  · HITL 4 场景             │ │
│                                          │  · 自进化蒸馏闭环          │ │
│                                          └────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

### 1.3 技术栈

| 技术 | 版本 | 用途 | 选型理由 |
|------|------|------|----------|
| Spring Boot | 2.3.12.RELEASE | 应用框架 | 稳定 LTS，兼容 JDK 1.8 |
| MyBatis-Plus | 3.4.3 | ORM + 分页 | Lambda 条件构造器简洁，单表 CRUD 零样板代码 |
| Spring Data Redis + Lettuce 6.1 | — | 8 种数据结构业务落地 + 分布式锁底层 | Lettuce 基于 Netty 异步、线程安全，比 Jedis 性能更优 |
| Redisson | 3.13.6 | 可重入分布式锁 + WatchDog | 比自研 setnx 锁更完善：可重入、自动续期、可重试 |
| Spring AMQP RabbitMQ | — | 秒杀异步下单 + 死信超时回滚 + 支付通知 + 退款 | 削峰填谷 + 解耦 + 死信天然支持延迟队列 |
| Spring Data ES + 原生 High Level Client | 7.17 | ES 索引建表 + synonym_graph 配置 + 查询 | High Level Client 支持原生 NativeSearchQuery 满足复杂查询 |
| Redis + Lua | — | 分布式令牌桶限流（`rate-limit.lua`） | 限流状态必须跨实例共享；Guava 的进程内 RateLimiter 已因此移除 |
| mysql-connector-java | 8.0.28 | JDBC 驱动 | 兼容 MySQL 8.0+ |
| Lombok / Hutool 5.7.17 | — | 代码简化 + 工具库 | BeanUtil / JSONUtil / StrUtil 提效 |
| IK Analyzer | 7.17 | ES 中文分词 | index=ik_max_word / search=ik_smart 双 analyzer |

> 选型对照（面试八股）：
> - **Redis+Lua 令牌桶 vs 单机 Guava RateLimiter**：Guava 的限流器是 JVM 进程内对象，部署 N 个实例阈值就放大 N 倍，且实例重启后桶是满的（每次发布白送一波突发）。本项目已改为 Redis + Lua 实现，状态跨实例共享。
> - **Redis+Lua vs Sentinel**：本项目要的是"阈值在多实例下成立"，Redis+Lua 已足够且无额外运维成本。规则动态推送自己也做了（§5.4：阈值写在 Redis 规则键里，Lua 当场读，改阈值零重启）。Sentinel 剩下的增量价值是**控制面的工程化**——规则的持久化与变更审计、Dashboard 可视化、热点参数限流；这三样都不需要改判定逻辑，切换点在"谁来写那个规则键"。
> - **ES vs MySQL LIKE**：MySQL `LIKE '%kw%'` 无法走索引、不支持分词；ES 基于倒排索引接近 O(1)，支持分词、高亮、相关度排序、聚合。
> - **synonym_graph vs 普通 synonym filter**：synonym_graph 保留多词条同义词（日本料理 = 日料）的位置偏移关系，不会产生假匹配，短语级 query 精度显著更好。
> - **Redisson vs 自研 setnx 锁**：自研锁不可重入、无续期、主从一致性有问题；Redisson 用 Hash 存储重入计数 + WatchDog 续期 + multiLock 红锁解决主从一致。

---

## 二、工程分层架构

```
src/main/java/com/hmdp/
├── HmDianPingApplication.java        # Spring Boot 启动类
├── annotation/                        # 自定义注解
│   ├── RateLimit.java                # @RateLimit 分布式令牌桶限流（含 failOpen 开关）
│   └── CircuitBreaker.java           # @CircuitBreaker 三态熔断器
├── aspect/                            # AOP 切面
│   ├── RateLimitAspect.java          # 限流切面，委托 RedisRateLimiter
│   └── CircuitBreakerAspect.java     # 熔断状态机
├── config/                            # 配置类
│   ├── ElasticsearchConfiguration.java  # synonyms.txt 加载 + index settings + DROP+CREATE+IMPORT
│   ├── RedissonConfig.java           # Redisson 客户端
│   ├── MvcConfig.java                # 拦截器注册 + 静态资源映射
│   ├── MybatisConfig.java            # MyBatis-Plus 分页插件
│   ├── QueueConfig.java              # RabbitMQ 队列/交换机/死信绑定
│   └── WebExceptionAdvice.java       # 全局异常处理器
├── controller/                        # 13 个 REST Controller
│   ├── UserController / ShopController / ShopTypeController / ShopSearchController
│   ├── VoucherController / VoucherOrderController / PaymentController / OrderController
│   ├── FollowController / BlogController / BlogCommentsController / RecommendController
│   └── UploadController
├── service/                           # 14 对 I*Service + impl
├── mapper/                            # MyBatis-Plus Mapper + VoucherMapper.xml
├── entity/                            # 数据库实体（与 tb_* 表对应）
│   ├── User / UserInfo / Shop / ShopType / Voucher / SeckillVoucher / VoucherOrder
│   ├── Blog / BlogComments / Follow
│   └── PayLog                        # 交易闭环新增：支付流水
├── document/                          # ES 文档实体
│   └── ShopDoc.java                  # 四字段都带 search_analyzer=shop_search_synonym
├── repository/                        # Spring Data ES Repository
│   └── ShopDocRepository.java
├── dto/                               # 数据传输对象
│   ├── Result.java                   # 统一响应封装 {success, data, errorMsg}
│   ├── LoginFormDTO / UserDTO / ScrollResult / PaymentDTO / ShopSearchResult
├── enums/                             # 状态枚举
│   ├── OrderStatus.java              # 订单状态机（UNPAID→PAID→VERIFIED / REFUNDING→REFUNDED / CANCELLED）
│   ├── PayType.java                  # 支付方式
│   └── BreakerState.java             # 熔断器三态 CLOSED/OPEN/HALF_OPEN
├── model/                             # 内存模型
│   └── BreakerInfo.java              # 熔断器统计信息（CAS 保证原子）
├── interceptor/                       # 双层拦截器
│   ├── RefreshTokenInterceptor.java  # order=0，全路径，刷新 Token TTL
│   └── LoginInterceptor.java         # order=1，只拦需登录路径
├── listener/                          # RabbitMQ 消费者
│   ├── SeckillVoucherListener.java   # 异步落库 + 发送延迟消息
│   ├── PayNotifyListener.java        # 支付通知
│   └── OrderDelayListener.java       # 死信超时取消
└── utils/                             # 工具类
    ├── CacheClient.java              # 三防工具（穿透/击穿/雪崩）
    ├── RedisIdWorker.java            # 雪花算法变种 ID 生成器
    ├── SimpleRedisLock.java          # 自研 setnx 锁（保留对比）
    ├── RedisConstants.java           # Key 前缀常量
    ├── RedisData.java                # 逻辑过期包装类
    ├── UserHolder.java               # ThreadLocal 当前用户
    ├── PasswordEncoder.java          # 密码加密
    └── RegexUtils / RegexPatterns / SystemConstants / ILock
```

### 2.1 分层职责

| 层 | 职责 | 关键约定 |
|---|---|---|
| Controller | 接收请求、参数校验、调用 Service、组装 Result | 不写业务逻辑，不直接调 Mapper |
| Service | 业务编排、事务边界、调用外部依赖 | 跨模块调用走接口（IPaymentService 等），加 @Lazy 打破循环依赖 |
| Mapper | 数据访问 | MyBatis-Plus 单表零样板，复杂查询走 XML（VoucherMapper.xml） |
| Listener | MQ 异步消费、补偿 | 处理失败不抛异常到死信，用幂等保护重试 |
| Aspect | 横切关注点（限流、熔断） | @Around 环绕通知，反射调 fallback 方法 |

---

## 三、关键技术实现

### 3.1 双层拦截器鉴权（Redis Token 替代 Session）

**问题**：传统 Session 不支持 Nginx 多实例集群共享；如果用 Session + sticky session 又限制伸缩性。

**方案**：Redis Token + 双层拦截器，将「刷新 TTL」与「鉴权拦截」职责分离。

```
请求 → RefreshTokenInterceptor (order=0, 全路径)
         ├─ 解析 Authorization → Redis HGETALL login:token:{token}
         ├─ 命中 → 刷新 30min TTL → 写入 UserHolder(ThreadLocal)
         └─ 没 token 也放行（不拦截，允许匿名浏览）
       → LoginInterceptor (order=1, 只拦 /blog /voucher-order /pay 等需登录路径)
         └─ UserHolder 非空 → 通过；否则 401
```

**关键点**：
- **RefreshTokenInterceptor 拦截全路径但只刷新不拦截**：所有请求（含静态资源、健康检查）都过它，因此匿名请求也能拿到 Token 续期；不会因路径白名单遗漏导致 Token 过期。
- **LoginInterceptor 只拦需登录路径**：商铺浏览、搜索等公开接口允许匿名访问，提升用户体验。
- **UserHolder 基于 ThreadLocal**：请求结束在 finally 中清理，避免线程池复用导致用户身份串号。
- **集群天然支持**：Token 存 Redis 而不是 JVM 内存，任意实例都能验证。

### 3.2 多级缓存与三防（CacheClient）

基于 Cache Aside 模式自研 `CacheClient` 工具类，针对缓存三大经典问题分别实施防御：

| 问题 | 定义 | 防御策略 | 实现 |
|------|------|----------|------|
| **穿透** | 查不存在的 id → DB 打穿 | 缓存空值 + 短 TTL | `cache:null:{id}` 设 2 分钟；DB 查不到也写空串，下次直接命中空值 |
| **雪崩** | 大量 Key 同时失效 → DB 瞬间压满 | TTL ±30% 随机抖动 | `setWithRandomExpire` 在基础 TTL 上叠加随机偏移 |
| **击穿** | 热点 Key 过期瞬间 → 成千请求打 DB | 逻辑过期 + 子线程异步重建 | RedisData 存逻辑时间；过期时 tryLock 仅一个线程重建，其他直接返回旧值 |

**逻辑过期核心代码逻辑**：
1. 读 Redis → 反序列化 RedisData，取 `expireTime`
2. 未过期 → 直接返回数据
3. 已过期 → `tryLock(LOCK_SHOP_KEY+id)` 获取互斥锁
   - 成功 → 提交到 `CACHE_REBUILD_EXECUTOR`（10 个线程的固定线程池）异步重建：查 DB → 写 Redis → 释放锁
   - 失败 → 别的线程在重建，直接返回旧数据
4. 用户始终拿到数据（可能旧一点），不会等待

**关键点**：
- **逻辑过期 vs 物理过期**：物理过期用 Redis EXPIRE，过期瞬间大量请求打 DB；逻辑过期把过期时间存在 value 里，Redis 永不过期，用户永远能拿到数据。
- **Double Check（双重检查）**：拿锁后应再检查一次缓存是否已被重建，避免等锁期间别的线程已完成重建。
- **空值判断细节**：`StrUtil.isNotBlank(json)` 区分 null（未缓存）、`""`（空值缓存）、有值三种状态，避免误判。

### 3.3 高并发秒杀（Redis + Lua + Redisson + RabbitMQ）

秒杀链路是面试 T0 考点，本项目实现工业界经典的「Redis 内存裁定 + MQ 异步落库」：

```
用户点击秒杀
  ↓
1. Redis + Lua 原子预检（seckill.lua）
   ├─ 判断 Redis 库存 > 0 → 否则返回 1（库存不足）
   ├─ 判断 Set 中是否已有 userId → 是则返回 2（一人一单）
   ├─ 扣减 Redis 库存（INCRBY stockKey -1）
   └─ SADD userId 到 Set → 返回 0（成功）
  ↓
2. RedisIdWorker 生成 orderId（时间戳 + 自增序列）
  ↓
3. 发送 RabbitMQ 消息到 X 交换机（路由 XA → QA 队列）
   ↓ 消息体：VoucherOrder JSON
4. SeckillVoucherListener 异步消费
   ├─ save(voucherOrder) 落库 DB
   ├─ seckillVoucherService.update() setSql("stock = stock - 1") where voucher_id=? and stock>0
   │   ── 乐观锁：仅 stock>0 时才扣减，防超卖
   └─ sendOrderDelayMessage(orderId) → 发送延迟消息到 ORDER_DELAY_QUEUE（TTL 30min）
  ↓
5. 30min 后消息过期 → 死信交换机 ORDER_DEAD_EXCHANGE → ORDER_CANCEL_QUEUE
  ↓
6. OrderDelayListener 消费
   └─ voucherOrderService.handleOrderTimeout(orderId)
       ├─ 查订单状态
       ├─ 仍是 UNPAID → 更新为 CANCELLED + restoreStockAndOrderRecord（Redis INCR + DB stock+1 + SREM userId）
       └─ 已 PAID → 忽略（用户已支付，幂等保护）
```

**关键技术决策**：

| 决策 | 理由 |
|---|---|
| **用 Lua 脚本而非分布式锁做预检** | 库存判定 + 一人一单 + 扣库存 + 写 Set = 4 个命令非原子不可；Lua 让 4 个操作一次 RTT 在 Redis 服务端串行执行，天然无并发竞态；分布式锁吞吐量低 |
| **异步落 DB** | DB 单行写 QPS 约 1k~3k，Redis 单 key 写可达 80k+；异步落库把峰值削平 |
| **死信回滚** | 防止用户抢到券但不支付一直占坑；TTL 30min 自动取消 + 恢复库存 |
| **消费端 Redisson 锁兜底** | 消息可能重复投递（网络抖动 ack 失败），消费端用 `lock:order:{userId}` 防重复下单 |
| **乐观锁防超卖** | `update set stock=stock-1 where voucher_id=? and stock>0`，MySQL 行锁保证只有一个线程扣减成功 |
| **@Lazy 打破循环依赖** | VoucherOrderService 依赖 PaymentService，PaymentService 又依赖 VoucherOrderService；用 @Lazy 注入 CGLIB 代理，延迟真实依赖解析 |

**RedisIdWorker 原理**：时间戳（秒级，2^32 秒 ≈ 136 年）+ 自增序列号（2^32 / 秒）。比数据库自增 ID 优势：①分库分表后不重复；②ID 不暴露订单量；③不依赖 DB。

### 3.4 支付退款闭环（PayLog + 死信超时 + 状态机）

交易生命周期完整闭环，解决训练项目「抢到券就结束」的半吊子问题：

```
订单状态机（OrderStatus 枚举）：
  ┌──────────┐    支付成功    ┌──────────┐    核销      ┌──────────┐
  │  待支付   │ ──────────→  │  已支付   │ ─────────→  │  已核销   │
  │  UNPAID  │              │   PAID    │             │ VERIFIED │
  └────┬─────┘              └─────┬────┘             └──────────┘
       │ 超时取消                │ 申请退款
       ▼                          ▼
  ┌──────────┐              ┌──────────┐    退款成功   ┌──────────┐
  │  已取消   │              │  退款中   │ ─────────→ │  已退款   │
  │CANCELLED │              │REFUNDING │             │ REFUNDED │
  └──────────┘              └──────────┘             └──────────┘

canTransitionTo() 封装状态机校验，防止非法跳转：
  UNPAID → PAID / CANCELLED
  PAID → VERIFIED / REFUNDING
  REFUNDING → REFUNDED
  VERIFIED / CANCELLED / REFUNDED → 终态
```

**支付接口链路**：
- `POST /pay` → PaymentService.payOrder：构建 PayLog（status=PAYING）→ 返回支付链接
- `POST /pay/notify` → 第三方回调：更新 PayLog（status=PAID）+ 更新订单 PAID + 发送支付通知 MQ
- `POST /pay/refund/{id}` → PaymentService.refundOrder：REFUNDING → 异步发退款 MQ → 退款回调 REFUNDED + 恢复库存

**PayLog 独立流水表的理由**：
1. 一个订单可能多次支付尝试（第一次失败，第二次成功）
2. 支付与订单是两个独立领域，职责分离
3. 对账需要：第三方支付平台流水号与本地流水要对得上
4. 退款也需要记录退款流水

**金额用 Long（分）而非 BigDecimal**：性能更好（基本类型 vs 对象）、无精度丢失、与微信/支付宝 API 单位一致、存储更省。

### 3.5 Elasticsearch 同义词检索（synonym_graph + IK 双 analyzer）

**索引 settings 关键配置**（由 `ElasticsearchConfiguration.buildIndexSettings()` 生成）：

```
analysis.filter.shop_synonyms.type       = synonym_graph     # 保留短语级位置偏移，精度优于普通 synonym
analysis.filter.shop_synonyms.expand     = true
analysis.filter.shop_synonyms.synonyms   = [日料,日本料理,日式,和食,寿司,刺身,居酒屋 ; 火锅,铜锅,涮锅,串串香,麻辣烫,冒菜 ; ...]  # 14 组

analysis.analyzer.shop_index_ik.tokenizer   = ik_max_word    # 建索引细粒度拆分，召回更广
analysis.analyzer.shop_search_synonym.tokenizer = ik_smart   # 搜索粗粒度拆分
analysis.analyzer.shop_search_synonym.filter  = [lowercase, shop_synonyms]  # 再同义词扩展召回
```

**字段 mapping**（name / area / address / tags 四字段）：
- `analyzer = shop_index_ik`（建索引用）
- `search_analyzer = shop_search_synonym`（搜索时同义词扩展）

**查询逻辑**（ShopSearchServiceImpl.search）：
```java
BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
// must：多字段全文搜索（参与评分）
boolQuery.must(QueryBuilders.multiMatchQuery(keyword,
        "name^3", "tags^2", "area^1.5", "address")  // 字段加权
        .type("most_fields"));
// filter：类型/商圈过滤（不参与评分，性能高，被 ES 缓存）
if (typeId != null) boolQuery.filter(QueryBuilders.termQuery("typeId", typeId));
if (StrUtil.isNotBlank(area)) boolQuery.filter(QueryBuilders.matchQuery("area", area));

NativeSearchQuery query = new NativeSearchQueryBuilder()
        .withQuery(boolQuery)
        .withPageable(PageRequest.of(current - 1, size))
        .withSort(SortBuilders.fieldSort("score").order(SortOrder.DESC))
        .withSort(SortBuilders.fieldSort("sold").order(SortOrder.DESC))
        .withHighlightBuilder(highlightBuilder)  // <em> 标签高亮
        .build();
```

**同义词真正写入 ES 的两种触发方式**（P0 级断口修复）：
- **方式 A**：`application.yaml: elasticsearch.init.rebuild-on-startup=true` → Spring Boot 启动时 ApplicationRunner 自动 DROP→CREATE→PUT MAPPING→IMPORT
- **方式 B**：`POST /shop/search/rebuild-index` 管理接口 → 服务运行期间零停服重建

**验证 filter 真的写入**：
```http
POST /shop/_analyze
{"analyzer":"shop_search_synonym","text":"寿司"}
→ tokens 至少包含：寿司 / 日料 / 日式 / 刺身 / 居酒屋
```

**深分页保护**：`from + size` 不能超过 10000（ES `index.max_result_window` 默认值）；超限直接返回错误，提示使用 `search_after` 方式。

**MySQL → ES 数据同步（写事务投事件 + 全量对账）**

历史上这里是个真断口：`ShopController.saveShop` 与 `ShopServiceImpl.update` 只写库、只删缓存，**完全不碰 ES**，也没有任何删文档的路径 —— 索引和 MySQL 静默漂移，只能靠启动时全量重导碰运气。现在拆成两层：

| 层 | 谁负责 | 时机 |
| --- | --- | --- |
| 增量 | `ShopServiceImpl.save/update` 在**业务事务内**投 `ES_SYNC` 事件 → `TransactionOutboxPublisher` 回查 MySQL 落索引 | 每次写入，秒级跟进 |
| 对账 | `POST /shop/search/rebuild-index`（或 `rebuild-on-startup=true`） | 改 mapping/同义词、事件进死信后的人工修复 |

三个设计点：

1. **事件只带 `shopId`，文档内容一律回查 MySQL。** `updateById` 只更新非空字段（实测：PUT 只提交 `name`+`avgPrice`，`area`/`type_id` 原值保留），把请求体里的半条实体拼成文档会把未提交字段写成 `null`；而带快照在事件乱序/重放时还会写回旧值。带 id 则永远取到当前值，重复投递天然幂等。
2. **`event_key` 必须每次唯一**（`es-sync:{shopId}:{RedisIdWorker}`）。事件表上有 `uk_event_key` 唯一索引，且 `TransactionOutboxWriter` 撞重复键只打一行 debug。若用 `es-sync:{shopId}` 做"幂等键"，同一商铺第二次更新会被静默吞掉、零报错。
3. **投递走 Outbox 内联分支，不投 MQ**（与 `REDIS_COMPENSATION` 同一形状）：消费者要做的事和发布器完全一样，中间那跳队列不提供任何东西；而本项目的队列都没配 DLX，`@RabbitListener` 抛异常 = 消息 requeue，ES 长期不通就是热循环刷日志。走 outbox 免费拿到指数退避 + 死信落表。

**已知局限**：① ES "慢而不通"时会拖住这条共用的扫描线程，`PAY_NOTIFY` 的延迟跟着涨（要隔离得给 `ES_SYNC` 单开调度池）；② 行被物理删除但没人投事件时，索引里的孤儿文档要等下次全量对账才清；③ 生产形态应是 Canal 订阅 binlog —— 切换点只有一个：把 `ES_SYNC` 事件的写入方从业务代码换成 Canal adapter，消费端 `syncShopById` 原样复用。

### 3.6 接口限流 + 熔断（AOP 自定义注解）

**两个自定义注解**：

| 注解 | 实现 | 使用场景 |
|---|---|---|
| `@RateLimit(qps=50, failOpen=false, message="活动太火爆了")` | RateLimitAspect + RedisRateLimiter（Redis + Lua 令牌桶，**跨实例、全站维度**） | 登录 5QPS / 秒杀 50QPS / 搜索 100QPS / 支付 20QPS |
| `@CircuitBreaker(failureThreshold=5, recoveryTimeout=30000, slidingWindow=60000, fallback="searchFallback")` | CircuitBreakerAspect + ConcurrentHashMap<方法名, BreakerInfo> | 商铺详情 fallback=MySQL / ES 搜索 fallback=MySQL LIKE |

**qps 的两级来源**（注解是默认值，Redis 是运行期覆盖）：

```
rate-limit.lua 第 0 步：GET config:ratelimit:rule:{api}
    能解析成数字且 >= 1  →  RATE / CAP 一起按它重算
    否则（键不存在/脏值/<1）→ 用 @RateLimit(qps=...)，行为与引入该键之前逐字节一致
```

| 决策 | 为什么这样选 |
|---|---|
| 覆盖值放 Redis 而不是配置中心/环境变量 | 所有实例读同一份，写一次全站生效；改注解 = 一次全量发布 + 重新预热缓存，为一个开关付这个不值 |
| 读取发生在 Lua 里，不在 Java 里读完传 ARGV | 限流是热路径，先 GET 再 EVAL 凭空多一个 RTT；更关键的是「读到的规则」与「写回的桶」之间不能有窗口，否则改规则的瞬间各实例各按新旧两套速率补同一个桶 |
| 覆盖只改 `RATE`/`CAP`，`BURST = CAP/RATE` 从 ARGV 反推 | 「突发窗口是几秒」这个决定只留在 Java 的 `MAX_BURST_SECONDS` 一处，Lua 不复制常量 |
| 阈值调小立刻见效 | `math.min(CAP, tokens + …)` 顺带把桶里攒下的旧令牌夹到新容量，不需要等它自然耗尽 |
| 非法值忽略而不是拒绝 | `qps < 1` 会让桶容量不足一个令牌 = 静默打死接口；`EXPIRE key 0` 之类的坑也只能靠"不让配置驱动它"来防。写入口 `/config` 有校验，走到这个分支只可能是有人直接 `redis-cli` 塞了脏值 |
| 删掉规则键 = 回到注解默认值 | 规则键只被读取、不被任何写入方回填，所以它永远是唯一事实源（把 qps 塞进令牌桶 Hash 的做法会被旧键里的旧值静默覆盖） |

**已知局限**：Redis Cluster 下 `KEYS[1]`（`ratelimit:api:`）与 `KEYS[2]`（`config:ratelimit:rule:`）前缀不同会落到不同 slot，脚本报 CROSSSLOT；届时的改法是两个键加同一个 hash tag（单机/主从无此问题）。运维入口见 §5.4。

**三态熔断器状态机**（BreakerState 枚举 + CAS 保证状态转换原子性）：

```
CLOSED → 正常放行，统计成功/失败
  连续 failureThreshold=5 次失败 → 跳 OPEN
OPEN → 直接 fallback，不打后端
  TTL 30s 后 → HALF_OPEN
HALF_OPEN → 放 1 个探针请求（compareAndSet probeSent=false→true 保证只放 1 个）
  探测成功 → CLOSED；任一失败 → 再 OPEN
```

**滑动窗口简化实现**：记录 `lastFailureTime`，距上次失败超过 `slidingWindow` 就重置计数器。精确版可用环形数组或 Redis ZSet。

**fallback 方法签名要求**：与原方法签名完全一致（参数列表、返回类型），通过反射调用。

**MySQL LIKE 兜底实现**（ShopSearchServiceImpl.searchFallback）：
```java
LambdaQueryWrapper<Shop> wrapper = new LambdaQueryWrapper<>();
if (StrUtil.isNotBlank(keyword)) {
    wrapper.and(w -> w.like(Shop::getName, keyword)
            .or().like(Shop::getArea, keyword)
            .or().like(Shop::getAddress, keyword));  // tags 在 MySQL 没单独列，用 area 替代
}
if (typeId != null) wrapper.eq(Shop::getTypeId, typeId);
if (StrUtil.isNotBlank(area)) wrapper.like(Shop::getArea, area);
wrapper.orderByDesc(Shop::getScore).orderByDesc(Shop::getSold);
```

### 3.7 推荐系统（协同过滤 + GEO + ZSet）

`RecommendServiceImpl` 实现三种推荐策略：

| 接口 | 策略 | Redis 数据结构 |
|---|---|---|
| `/recommend/shops` | **User-based 协同过滤** | `user:liked:shops:{userId}` Set（用户点赞商铺集合）+ `shop:liked:users:{shopId}` Set（商铺点赞用户集合） |
| `/recommend/nearby` | **附近热门** | `shop:geo:{typeId}` GEO（GESEARCH 5km 半径）+ MySQL score/sold 排序 |
| `/recommend/hot` | **全站热榜** | `shop:hot` ZSet（score=sold，ZREVRANGE 取 Top-N） |

**协同过滤实现思路**（简化版 Jaccard 相似度）：
1. 获取当前用户点赞的所有商铺 ID（Set SMEMBERS）
2. 对每个点赞商铺，找也点赞过该商铺的其他用户（SINTER 求交集）
3. 收集所有相似用户点赞过的商铺，排除当前用户已点赞的
4. 按"被多少个相似用户点赞"计数排序，取 Top-10 推荐
5. **冷启动处理**：无点赞记录或无相似用户 → 退化为全站热门（ZSet）

**GEO 实现要点**：
- 底层是 ZSet，GeoHash 算法将经纬度编码为 score
- `GEOADD shop:geo:{typeId} x y shopId` 添加商铺
- `GEOSEARCH shop:geo:{typeId} FROMLONLAT x y BYRADIUS 5000 m` 搜索半径内商铺
- 返回结果按距离排序，本地按 score/sold 二次重排

### 3.8 Redis 8 种数据结构应用全景

| 数据结构 | 应用场景 | 关键 Key 前缀 | 关键命令 |
|----------|----------|--------------|----------|
| **String** | 商铺缓存（逻辑过期防击穿）、验证码、Token、分布式锁、短信限流计数 | `cache:shop:{id}`, `login:token:`, `login:code:`, `ratelimit:sms:cooldown:{phone}`, `ratelimit:sms:daily:{phone}`, `ratelimit:sms:global` | SET（带 EX NX） / GET / INCR |
| **Hash** | 用户信息（多字段）、接口限流令牌桶（`{tokens, ts}` 两个字段） | `login:token:{token}`, `ratelimit:api:{全限定类名}.{方法名}` | HSET / HGETALL / HMGET |
| **Set** | 关注列表、共同关注（交集）、秒杀一人一单记录、协同过滤相似用户 | `follows:{userId}`, `seckill:order:{voucherId}`, `user:liked:shops:{userId}` | SADD / SINTER / SREM / SMEMBERS |
| **ZSet** | 点赞排行榜（时间戳作为 score 天然排序）、全站热门商铺 | `blog:liked:{blogId}`, `shop:hot` | ZADD / ZREVRANGE / ZRANGEBYSCORE |
| **GEO** | 附近商户搜索 | `shop:geo:{typeId}` | GEOADD / GEOSEARCH |
| **BitMap** | 用户签到（按月 Bitmap） | `sign:{userId}:{yyyyMM}` | SETBIT / BITCOUNT / BITFIELD |
| **HyperLogLog** | UV 独立访客统计（百万级 UV 仅需 12KB） | `uv:page:{pageId}` | PFADD / PFCOUNT |
| **Lua 脚本** | 秒杀原子预检 | `seckill.lua` | EVALSHA |

---

## 四、数据库设计

### 4.1 核心表清单

| 表 | 文件 | 用途 |
|---|---|---|
| `tb_user` | sql/backend/schema/001_core.sql | 用户主表（手机号、密码、昵称） |
| `tb_user_info` | 同上 | 用户详情（生日、城市、积分、等级） |
| `tb_shop` | 同上 | 商铺（name/typeId/area/address/x/y/avgPrice/sold/comments/score） |
| `tb_shop_type` | 同上 | 商铺类型（美食/KTV/亲子…） |
| `tb_voucher` | 同上 | 优惠券（普通券 + 秒杀券共用） |
| `tb_seckill_voucher` | 同上 | 秒杀券扩展（stock/beginTime/endTime） |
| `tb_voucher_order` | 同上 | 优惠券订单（userId/voucherId/status/payType） |
| `tb_blog` | 同上 | 探店笔记（shopId/userId/title/content/images/liked） |
| `tb_blog_comments` | 同上 | 评论（blogId/parentId/answerId/content） |
| `tb_follow` | 同上 | 关注关系（userId/followUserId） |
| `tb_pay_log` | sql/backend/schema/002_payment_and_search.sql | **支付流水**（交易闭环新增：orderId/payType/tradeNo/amount/status） |
| `tb_agent_preferences` | sql/agent2/schema/agent2_tables.sql | Agent 用户 9 维偏好记忆（JSON） |
| `tb_agent_playbook` | 同上 | Agent 全局经验库（fuzzy_mapping 条目） |
| `tb_agent_conversations` | 同上 | Agent 会话对话历史 |
| `tb_agent_trajectories` | 同上 | Agent 执行轨迹（节点日志 + decisions + outcome） |

### 4.2 关键设计决策

| 决策 | 理由 |
|---|---|
| **金额用 Long（分）** | 性能优于 BigDecimal、无精度丢失、与第三方支付 API 单位一致 |
| **score 字段乘 10 存整数** | MySQL 整数运算快于浮点；避免小数精度问题；展示时除以 10 |
| **秒杀券独立表 tb_seckill_voucher** | 与 tb_voucher 一对一关系；扩展库存/时间字段不污染普通券 |
| **订单状态用枚举 + canTransitionTo** | 类型安全、可扩展、状态机规则集中 |
| **PayLog 独立于订单表** | 一个订单可能多次支付尝试；支付与订单是独立领域；对账需要 |
| **Agent 4 张表用 MySQL JSON 字段** | preferences JSON 灵活适应 9 维偏好演化；version 字段支持 schema 升级 |

### 4.3 索引设计

- `tb_shop`：主键 `id` + `(type_id, score, sold)` 联合索引（按类型查热门）
- `tb_voucher_order`：主键 `id` + `(user_id, voucher_id)` 唯一索引（一人一单 DB 层兜底）+ `(user_id, status, create_time)` 复合索引（查询我的订单）
- `tb_blog`：主键 `id` + `shop_id` 索引（按商铺查笔记）+ `liked` 索引（按点赞排序）
- `tb_pay_log`：主键 `id` + `order_id` 索引（按订单查流水）+ `trade_no` 唯一索引（第三方对账）
- `tb_agent_trajectories`：主键 `trajectory_id` + `user_id` 索引 + `created_at` 索引

---

## 五、配置文件与启动流程

### 5.1 application.yaml 关键配置

```yaml
server:
  port: 8081

spring:
  datasource:
    url: jdbc:mysql://127.0.0.1:3306/dingping?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true
    username: root
    # password 留空 — 本地 MySQL 无密码
  redis:
    host: 127.0.0.1
    port: 6379
    lettuce:
      pool:
        max-active: 30
        max-idle: 15
        min-idle: 5
        time-between-eviction-runs: 10s
  rabbitmq:
    host: localhost
    port: 5672
    username: guest
    password: guest
  jackson:
    default-property-inclusion: non_null

elasticsearch:
  rest:
    uris: http://127.0.0.1:9200
  init:
    # 【关键】启动时强制 DROP+重建 shop 索引（含 synonym_graph filter + IK 双 analyzer）
    # 修改 synonyms.txt / analyzer 配置 后设为 true 重启一次即可完成重建
    # 之后可改回 false 避免每次重启都删数据重导，或调 POST /shop/search/rebuild-index
    rebuild-on-startup: true
    shards: 1
    replicas: 0

mybatis-plus:
  type-aliases-package: com.hmdp.entity

logging:
  level:
    com.hmdp: debug
```

### 5.2 启动流程

1. Spring Boot 启动 → 加载 application.yaml
2. MybatisConfig 注册分页插件
3. RedissonConfig 创建 RedissonClient
4. MvcConfig 注册双层拦截器（RefreshTokenInterceptor order=0、LoginInterceptor order=1）+ 静态资源映射
5. QueueConfig 声明 RabbitMQ 交换机/队列/绑定（X/Y 普通/死信 + 订单延迟/取消 + 支付通知 + 退款）
6. ElasticsearchConfiguration 加载 `classpath:synonyms.txt`（14 组同义词）→ 构建 index settings
7. ApplicationRunner（若 `rebuild-on-startup=true`）：DROP shop 索引 → CREATE → PUT MAPPING → importAllShops()
8. HmDianPingApplication 监听 :8081

### 5.3 ES 同义词热更新（零停服）

修改 `src/main/resources/synonyms.txt` 后无需重启 Java：

```bash
curl -X POST http://127.0.0.1:8081/shop/search/rebuild-index
```

ShopSearchController → ShopSearchServiceImpl.rebuildIndex → ElasticsearchConfiguration.rebuildIndexInternal(force=true)：
1. DROP shop 索引（存在就删）
2. CREATE shop（含最新 synonym_graph filter）
3. PUT MAPPING（name/area/address/tags 四字段 mapping）
4. importAllShops()：从 tb_shop 查全量 → 转 ShopDoc → saveAll 批量写入 ES

---

### 5.4 运行期动态配置（零重启）

两类值可以在不发布的前提下改掉，都存在 Redis 的 `config:` 命名空间下，读写都走 `DynamicConfig`：

| 配置 | 键 | 合法区间 | 越界/脏值时 |
|---|---|---|---|
| 接口限流阈值 | `config:ratelimit:rule:{全限定类名.方法名}` | `[1, 100000]` | 忽略覆盖，用注解值 |
| Outbox 扫描间隔 | `config:outbox:publish-interval-ms` | `[200, 10000]` ms | 忽略覆盖，用 `transaction.outbox.publish-interval-ms` |

**管理接口**（需要登录态；`/config/**` 不在 `MvcConfig` 的登录白名单里，所以未登录 401，但**任何登录用户都能改**——本项目没有角色体系，与 `MvcConfig` 里记的"白名单按前缀放行过宽"是同一条缺陷的两个方向）：

```bash
curl -s -H "authorization: $TOKEN" http://127.0.0.1:8081/config          # 三列总览
curl -s -X PUT  -H "authorization: $TOKEN" \
  "http://127.0.0.1:8081/config/ratelimit?api=com.hmdp.controller.UserController.login&qps=1"
curl -s -X DELETE -H "authorization: $TOKEN" \
  "http://127.0.0.1:8081/config/ratelimit?api=com.hmdp.controller.UserController.login"
curl -s -X PUT -H "authorization: $TOKEN" "http://127.0.0.1:8081/config/outbox-interval?ms=200"
```

`api` 走 query 参数而不是路径变量（值里全是点，放路径里会被后缀模式匹配截掉一段），且需要 URL 编码 —— 实测 `?api=a{b}.c` 会被容器归一化成 `ab.c`，所以**响应里回显的 `api` 才是真正写进去的键**。

**「扫描间隔」怎么做到不改 `@Scheduled` 也能变**：Spring 5.2 没有自适应 trigger，`fixedDelayString` 只在启动时解析一次。所以发布器以最小粒度 200ms 空转（`SCAN_TICK_MS`，与可调下限同源），每轮开头读一次间隔键决定"到点没有"：

- 多出来的是每轮一次 Redis GET，换来的是积压时能把间隔从 1s 立刻压到 200ms 追平。
- 心跳指标 `dianping_outbox_last_scan_age_seconds` 在**门控之前**更新，所以它量的是"调度线程活着"，把间隔调大不会撞上 `> 30s` 的「publisher 卡死」告警；真卡死时 tick 根本不会发生，告警照旧有效。
- 上限只到 10s：再长就是"暂停投递"，而那种状态在监控上长得和"一切正常"一样，该用停实例这种显式手段。

**实测（单机，本地 Redis/MySQL 均在跑）**：

| 动作 | `/user/login` 一批 40 并发里的放行数 | Outbox 一条新事件从落库到被投递的等待 |
|---|---|---|
| 注解 `qps=5` | 5 | — |
| `PUT …qps=1` | **1** | — |
| `DELETE` 覆盖 | 5（立刻回到注解值） | — |
| `PUT /config/outbox-interval?ms=10000` | — | **8.87s** |
| `PUT /config/outbox-interval?ms=200` | — | **0.27s** |

三次阈值操作全程没有重启，`GET /config` 回显的 `effectiveQps` 与实测放行数逐条对得上。

---

## 六、API 端点设计

### 6.1 用户模块

| 方法 | 路径 | 说明 | 保护 |
|------|------|------|------|
| POST | `/user/code` | 发送验证码 | `SmsRateLimiter`：按手机号 60s 冷却 + 每日 10 条 + 全站兜底（Redis + Lua） |
| POST | `/user/login` | 登录（手机号 + 验证码） | `@RateLimit(5 QPS)` |
| POST | `/user/logout` | 清 Redis Token | 需登录 |
| GET  | `/user/me` | 当前用户信息 | 需登录 |
| GET  | `/user/sign` | 当日签到（BitMap） | 需登录 |
| GET  | `/user/sign/count` | 连续签到天数（BITCOUNT） | 需登录 |

### 6.2 商户与搜索

| 方法 | 路径 | 说明 | 保护 |
|------|------|------|------|
| GET  | `/shop/{id}` | 商户详情 | `@CircuitBreaker(fallback=MySQL)` |
| GET  | `/shop-type/list` | 类型列表 | Redis 缓存 |
| GET  | `/shop/of/type` | 类型查询（MySQL 兜底） | — |
| GET  | `/shop/of/name` | 名称查询（MySQL 兜底） | — |
| GET  | `/shop/search` | **ES 全文搜索（IK + synonym_graph + 高亮）** | `@CircuitBreaker(fallback=MySQL LIKE)` |
| POST | `/shop/search/sync` | 全量同步 tb_shop → ES | 需登录 |
| POST | `/shop/search/import` | 单条导入 ES | — |
| POST | `/shop/search/rebuild-index` | **同义词强制重建索引（零停服）** | 需登录 |

### 6.3 优惠券与秒杀

| 方法 | 路径 | 说明 | 保护 |
|------|------|------|------|
| POST | `/voucher/seckill` | 新增秒杀券 | 需登录 |
| POST | `/voucher-order/seckill/{id}` | 秒杀下单（Lua + MQ） | `@RateLimit(50 QPS)` + 分布式锁 |

### 6.4 支付与订单

| 方法 | 路径 | 说明 | 保护 |
|------|------|------|------|
| POST | `/pay` | 发起支付 | 需登录 |
| POST | `/pay/notify` | 支付回调 | 第三方 |
| POST | `/pay/refund/{id}` | 申请退款 | 需登录 |
| POST | `/pay/refund/callback` | 退款回调 | 第三方 |
| GET  | `/order/{id}` | 订单详情 | 需登录 |
| GET  | `/order/list` | 我的订单（按状态筛选） | 需登录 |
| POST | `/order/cancel/{id}` | 手动取消订单（恢复库存） | 需登录 |

### 6.5 社交

| 方法 | 路径 | 说明 | 保护 |
|------|------|------|------|
| PUT  | `/follow/{id}/{isFollow}` | 关注/取关 | 需登录 |
| GET  | `/follow/common/{id}` | 共同关注（Set 交集） | 需登录 |
| POST/PUT/GET | `/blog` / `/blog/hot` / `/blog/like/{id}` | 探店笔记 / 热榜 / 点赞（ZSet） | 写需登录 |
| POST/GET | `/blog-comments/*` | 评论 CRUD + 点赞 | 写需登录 |

### 6.6 推荐

| 方法 | 路径 | 说明 | 保护 |
|------|------|------|------|
| GET  | `/recommend/shops` | 协同过滤（基于 Set 交集） | — |
| GET  | `/recommend/nearby` | 附近热门（GEO + score/sold 排序） | — |
| GET  | `/recommend/hot` | 全站热榜（ZSet） | — |

### 6.7 运行期配置（`ConfigController`）

| 方法 | 路径 | 说明 | 保护 |
|------|------|------|------|
| GET | `/config` | 总览：每一项动态配置的「注解/默认值 · 覆盖值 · 实际生效值」三列 | `@RateLimit(10 QPS)` + 需登录 |
| GET | `/config/ratelimit?api=` | 单个接口的限流规则；既没配过也不在登记表里时报「未配置」，不回显空行 | 同上 |
| PUT | `/config/ratelimit?api=&qps=` | 覆盖 qps，全部实例下一个请求起生效；越界（`<1` 或 `>100000`）拒绝且不写任何键 | 同上 |
| DELETE | `/config/ratelimit?api=` | 删除覆盖 = 回到注解默认值 | 同上 |
| PUT | `/config/outbox-interval?ms=` | 调整 Outbox 扫描间隔，合法区间 `[200, 10000]` ms | 同上 |
| DELETE | `/config/outbox-interval` | 回到 `transaction.outbox.publish-interval-ms` | 同上 |

约定两点（细节见 §5.4）：`api` 只能走 query 参数（值里全是点，放路径会被 Spring MVC 的后缀匹配截掉最后一段），响应回显的 `api` 才是真正写进 Redis 的键；`/config/**` 只做了「需登录」，没有角色校验 —— 与 `MvcConfig` 白名单过宽是同一条已知缺陷，切换点在 `LoginInterceptor` 之后加一个角色拦截器。

### 6.8 统一响应格式

```java
// Result.java
{
  "success": true,
  "data": {...},
  "errorMsg": null
}
```

全局异常处理由 `WebExceptionAdvice` 统一捕获 `RuntimeException` → `Result.fail()`，避免栈信息泄漏给前端。

---

## 七、RabbitMQ 队列与交换机设计

### 7.1 队列拓扑（QueueConfig）

```
秒杀异步下单：
  X (DirectExchange) ──[XA]──→ QA (TTL 10s) ──[死信]──→ Y (DirectExchange) ──[YD]──→ QD
                                                                                    ↓
                                                                            SeckillVoucherListener.receivedD (兜底)

订单延迟取消：
  ORDER_DELAY_EXCHANGE ──[order.delay]──→ ORDER_DELAY_QUEUE (TTL 30min)
       ↓ [消息过期成为死信]
  ORDER_DEAD_EXCHANGE ──[order.dead]──→ ORDER_CANCEL_QUEUE
       ↓
  OrderDelayListener.handleOrderCancel → handleOrderTimeout

支付通知：
  PAY_NOTIFY_EXCHANGE ──[pay.notify]──→ PAY_NOTIFY_QUEUE → PayNotifyListener

退款：
  REFUND_EXCHANGE ──[refund]──→ REFUND_QUEUE → 退款消费者
```

### 7.2 死信队列三种来源（八股）

1. 消息被拒绝（basic.reject / basic.nack）且 requeue=false
2. 消息 TTL 过期（本项目使用）
3. 队列达到最大长度

### 7.3 per-message TTL 的队头阻塞问题

队列级 TTL（`x-message-ttl`）有队头阻塞：如果队头消息 TTL=30min，后面消息 TTL=10s，后面消息即使先过期也要等队头过期才会被检查。本项目所有消息 TTL 相同（30min），不存在此问题；若需差异化 TTL，应用 `rabbitmq_delayed_message_exchange` 插件。

### 7.4 消费幂等性设计

| 场景 | 幂等手段 |
|---|---|
| 秒杀消息重复投递 | 消费端 `redissonClient.getLock("lock:order:" + userId).tryLock()` 防重复下单 |
| 延迟取消消息重复 | `handleOrderTimeout` 检查订单状态：仍是 UNPAID 才取消，已 PAID 直接忽略 |
| 支付回调重复 | 检查 PayLog.status，已 PAID 不再更新 |

---

## 八、高可用与降级策略

### 8.1 防御矩阵速查

| 风险点 | 防御手段 | 位置 |
|---|---|---|
| 缓存穿透 | 缓存空值 + 短 TTL 2min | CacheClient.queryWithPassThrough |
| 缓存击穿 | 逻辑过期 + 子线程异步重建 + 互斥锁 | CacheClient.queryWithLogicalExpire |
| 缓存雪崩 | TTL ±30% 随机抖动 | CacheClient.setWithRandomExpire |
| 秒杀超卖 | Lua 原子预检 + Redisson WatchDog + 乐观锁（stock>0） | VoucherOrderServiceImpl |
| 订单抢了不支付占坑 | 死信队列 TTL 30min + 超时自动取消并回库 | OrderDelayListener |
| ES 宕机搜不到 | @CircuitBreaker fallback MySQL LIKE 4 字段兜底 | ShopSearchServiceImpl.searchFallback |
| ES 同义词不生效 | rebuild-on-startup=true + rebuild-index 管理接口 + _analyze 验证 | ElasticsearchConfiguration |
| 商铺详情 DB 压力 | @CircuitBreaker fallback MySQL 直查 | ShopServiceImpl |
| 接口被恶意刷 | @RateLimit 令牌桶（秒杀 50QPS / 登录 5QPS / 搜索 100QPS / 支付 20QPS） | RateLimitAspect |
| 短信被刷爆（按号码循环调用） | 按手机号 60s 冷却 + 每日 10 条 | SmsRateLimiter（Redis + Lua） |
| 短信被刷爆（多号码枚举轰炸） | 全站兜底闸门，按预算配置额度 | SmsRateLimiter 全局计数键 |
| 接口限流单机失效 | 限流状态放 Redis 而非 JVM 内存，多实例阈值不放大 | RedisRateLimiter + rate-limit.lua |
| 限流器 Redis 故障时每个请求都付超时代价 | 连续失败 3 次后本地熔断 10 秒，期间不访问 Redis | RedisRateLimiter 内置熔断 |
| 熔断器状态转换竞态 | ConcurrentHashMap + AtomicReference + compareAndSet | CircuitBreakerAspect |
| 循环依赖启动失败 | @Lazy 注入 CGLIB 代理，延迟真实依赖解析 | VoucherOrderServiceImpl.paymentService |
| LLM 推理超时拖垮 Java | Agent 独立微服务部署，HTTP 调用带超时 | agent-services 独立进程 |
| Agent 数据访问失败 | HTTP 调 Java，Java 层 Redis 缓存兜底 | shop_api_http.py |

### 8.2 限流 vs 熔断 vs 降级

| 概念 | 时机 | 目的 | 本项目实现 |
|---|---|---|---|
| **限流** | 请求入口 | 保护服务不被打挂 | @RateLimit 令牌桶 |
| **熔断** | 调用下游失败 | 防止级联故障 | @CircuitBreaker 三态熔断器 |
| **降级** | 熔断后 / 主动 | 保证核心可用 | fallback 方法（MySQL LIKE 兜底 ES） |

### 8.3 ES 同义词 + 熔断器组合高可用

- 常态：`GET /shop/search` → ES multiMatchQuery（4 字段 + 高亮 + BM25 评分）→ 返回
- ES 宕机或连续 5 次失败：@CircuitBreaker CLOSED → OPEN 30s，期间所有 search 走 searchFallback：MySQL LIKE name/area/address/tags 四字段 + 评分倒排
- 30s 后 HALF_OPEN 放行 3 条探针：全部成功 → CLOSED；任一失败 → 再 OPEN
- 对前端/Agent 透明：返回格式一致，调用方不需要知道底层是 ES 还是 MySQL

---

## 九、与 Agent 微服务的协作协议

### 9.1 数据访问

Agent 微服务通过 HTTP 直连 Java 后端，复用 Java 层所有能力（Redis 缓存、ES 同义词、限流熔断、GEO 等），无需在 Python 侧重写：

| Agent 工具 | 调用 Java 接口 | 复用能力 |
|---|---|---|
| `search_shops_by_keyword` | `GET /shop/search` | ES synonym_graph 同义词扩展 + 熔断器 + 高亮 |
| `search_shops_nearby` | `GET /shop/of/type` + Redis GEO | GEO 5km 半径 + 商铺类型映射 |
| `get_shop_detail` | `GET /shop/{id}` | 商铺缓存（逻辑过期防击穿） |
| `get_shop_types` | `GET /shop-type/list` | 类型列表 Redis 缓存 |
| `get_shop_reviews` | `GET /blog/of/shop` | 笔记按 liked 排序 |

### 9.2 Redis 共享

Java 与 Agent 共用同一 Redis 实例，按 Key 前缀划分：

| Key 前缀 | 所有者 | 用途 |
|---|---|---|
| `login:token:` | Java | 用户登录 Token |
| `cache:shop:` | Java | 商铺缓存 |
| `shop:geo:` `shop:hot` `seckill:*` `ratelimit:sms:*` `ratelimit:api:*` | Java | GEO / 热榜 / 秒杀 / 短信限流 / 接口限流 |
| `agent1:summary:` | Agent1 | 评价摘要缓存（TTL 30min） |
| `agent2:memory:` `agent2:distill:` | Agent2 | 用户偏好 / 蒸馏队列 |
| `conversation:` | Agent2 | 会话上下文 |

### 9.3 MySQL 共享

- Java 用 `dingping` 数据库的 `tb_*` 表（核心业务）
- Agent 用同一 `dingping` 数据库的 `tb_agent_*` 表（4 张记忆表）
- 跨服务查询通过 HTTP 接口，不直接读对方表

### 9.4 错误码与超时重试

- Java 统一返回 `Result{success, data, errorMsg}`
- Agent HTTPX 调用带 5s 超时 + 1 次重试
- Agent 工具失败时 execute_node 把错误信息塞回 state，由 evaluate 规则决定是否 replan_relax

---

## 十、性能与扩展性

### 10.1 关键性能指标

指标出口：`GET /actuator/prometheus`（Micrometer + Prometheus 文本格式，无需登录，见 `MvcConfig` 白名单）。

采集与看板在 `observability/`：`docker compose up -d` 起 Prometheus（127.0.0.1:9090）+ Grafana（127.0.0.1:3000，目录「点评后端」，面板 uid `dianping-overview`）。JSON 里的每条查询、`prometheus/alerts/dianping.yml` 里的 7 条告警规则，与下表是同一套口径，改这里要同步改那里。

| 指标 | 估算区间 | 测试方法 |
|---|---|---|
| 秒杀 QPS | 4000-6000 | JMeter `seckill.jmx`；服务端对照 `sum(rate(dianping_seckill_precheck_total[$__rate_interval]))` |
| 秒杀 P99 延迟 | 150-250ms | JMeter 聚合报告 99% Line（客户端视角）；服务端 `histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket{uri="/voucher-order/seckill/{id}"}[$__rate_interval])))` |
| 商铺详情缓存命中率 | 95%-98.5% | `sum(rate(dianping_cache_lookup_total{result=~"hit\|hit_plain\|null_hit"}[5m])) / sum(rate(dianping_cache_lookup_total[5m]))` |
| ES 搜索 P95 延迟 | 30-80ms | `histogram_quantile(0.95, sum by (le) (rate(http_server_requests_seconds_bucket{uri="/shop/search"}[$__rate_interval])))` |
| 同义词召回率提升 | 2-5× | 人工构造 20 个同义 query 看返回交集并集 |

> 表里的 `uri` 标签值是实测的 Spring 路径模板（`/voucher-order/seckill/{id}`、`/shop/{id}`），不是猜的 —— 这一层写错，面板只会安静地空着。`observability/check-queries.py` 就是拿一份真实抓取的文本去核对 JSON 里所有指标名与标签的，起栈之前先跑它。

**口径说明**（比数值更容易读错，写在指标旁边）：

- `http_server_requests` 开了 percentile histogram（实测导出 69 个 `le` 桶），所以服务端 P99 与 JMeter 的客户端 P99 是**两个数**。限流打满时前者明显低于后者：被拒请求在服务端几乎零耗时，突发排队体现在客户端。以前只有 `result.jtl` 一个数，现在两者的差值本身就是限流保护效果的度量。
- 一次 `/shop/{id}` 请求可能落进两个档：`not_cached`（逻辑过期查不到）+ `miss_db_found`（兜底走穿透回填），因此命中率分母是**查缓存次数**而不是请求数；`null_hit` 计入命中（它不查 DB）。
- `hit` 与 `hit_plain` 都是命中，区别在值格式：前者是逻辑过期信封 `RedisData`（预热写入），后者是 `queryWithPassThrough` 回填的裸 JSON + 物理 TTL。两种策略共用同一个 key，`hit_plain` 持续走高就说明热点 key 实际靠物理过期在撑、预热没到位。
- Outbox 死信看 `increase(dianping_outbox_event_total{result="dead"}[1h])`：出现即说明有事件重试耗尽（永久失败，例如补偿目标已不存在），需要人工决定重放还是废弃；`dianping_outbox_stuck_recovered_total` 则对应"实例在已抢占未投递之间死掉过"。
- 熔断器与限流的 outcome 是**必拆的**：`rejected` 与 `unavailable_rejected` 对客户端是同一个响应体，只有指标能区分"限流生效"和"Redis 挂了导致失败关闭"。
- **所有 `dianping_*` 指标都是懒注册的**：没走过的代码路径不会有序列，面板上的 No data 不等于 0。同理，被 `@RateLimit` 挡在切面外层的请求不会进 `dianping_seckill_precheck_total`，所以"预检 QPS"永远 ≤ 接口 QPS，两者的差就是限流拦掉的那部分。
- **异步落库收敛时间仍测不出来**：秒杀走 Redis Stream，消费端没有埋耗时指标，"500 单约 4 秒落库"这类数只能轮询 DB（`stress/verify.sh`）。这是当前观测面的一个真实缺口 —— 想补就给它加一个 `Timer`，别再靠日志时间戳相减。

### 10.2 横向扩展预留

- **Redis 集群**：Token、缓存、锁全部走 Redis，集群模式下 Redisson 自动适配（multiLock 红锁解决主从一致）
- **MySQL 读写分离**：MyBatis-Plus 支持动态数据源切换
- **ES 集群**：当前单节点（shards=1, replicas=0），生产应至少 3 节点 + 副本
- **RabbitMQ 集群**：3 节点镜像队列保证高可用
- **Java 多实例**：拦截器 + Redis Token 天然支持 Nginx 多实例集群
- **Agent 微服务**：可单独扩副本 / 走 GPU 节点，与 Java 解耦

### 10.3 链路追踪（traceId）

指标只能定位到"哪个接口、哪一类结果"。要回答"这一次请求卡在哪一步"，得把它的日志单独捞出来 —— 而本项目的业务普遍跨线程，MDC 是个 ThreadLocal，**不会自动跨线程**，所以每个切换点都要显式接力（`utils/TraceContext`）：

| 切换点 | 接法 | 代码位置 |
|---|---|---|
| 客户端 → HTTP 线程 | 读 `X-Trace-Id`（非法则自己生成），并回写响应头 | `config/TraceFilter` |
| HTTP 线程 → Redis Stream 消费者 | id 作为**消息字段**随 Lua 原子落进 Stream（Stream 没有 header，字段就是全部元数据） | `seckill.lua` + `SeckillVoucherListener` |
| 生产者 → RabbitMQ 消费者 | 所有发送统一在出口处写消息头，业务侧无感 | `ConfirmedRabbitPublisher` |
| HTTP 线程 → 缓存重建线程池 | `TraceContext.wrap()` 在**提交处**抓快照（包 ThreadFactory 只能改线程名，改不了 MDC） | `CacheClient` |
| Outbox 投递（`@Scheduled` 线程） | 无上游，每条事件起一个 id，再经消息头传下去 | `TransactionOutboxPublisher` |

日志侧在 `logback-spring.xml` 的默认格式里插了 `[%X{traceId:-no-trace}]`。`no-trace` 是有意保留的：Hikari / Redisson / Tomcat 接收线程的日志本来就不属于任何请求，把它们和"链路断了"混成一个空括号会误导。

```bash
curl -X POST -H "authorization: $TOKEN" -H "X-Trace-Id: my-debug-01" \
     http://localhost:8081/voucher-order/seckill/105
grep 'my-debug-01' app.log      # HTTP 线程 + seckill-order-consumer + MQ 容器线程的日志一起出来
```

三条边界，不知道就会误信这个数：

- **入站 id 必须过白名单** `[A-Za-z0-9_-]{8,64}`。traceId 会原样进日志，不校验等于允许调用方往日志里塞东西（curl 本身不允许把头值写成换行，但网关和脚本会解码后再传，所以按字符集校验，不依赖调用方自觉）。实测：`X-Trace-Id: fake%0AEVIL` 被换成一个新生成的 16 位 id，响应头回写的也是新值。
- **Outbox 落表这一跳不接原始 id**：表里没有 trace 列（加列要走 DDL，项目目前没有迁移工具），写事件的是业务线程、投递的是扫描线程，中间隔一次落表。跨这一跳请用 payload 里的 `orderId` / `tradeNo` grep —— 那本来就是业务主键，比 traceId 更好用。
- **有 id 不代表有日志**：秒杀成功路径全程零日志（只有降级/异常才打），所以"grep 不到东西"是正常现象。实测验证走的是消息头：带 `X-Trace-Id: phase2-seckill-01` 下单后，`order.delay.queue` 里那条延迟消息的 header 就是同一个值（管理 API `POST /api/queues/%2F/order.delay.queue/get` + `ack_requeue_true`，只读不消费），说明 HTTP → Lua → Stream 消费者 → MQ 四棒全部接上。

再往上一档（跨服务、Java ↔ Agent 同一条 trace、采样与拓扑图）就该上 SkyWalking / Jaeger 了，见 §十二 第 8 条。

---

## 十一、文档索引

| 文档 | 说明 |
|------|------|
| [README.md](../README.md) | 项目总览 + 快速开始 + 架构亮点 |
| [docs/SETUP.md](SETUP.md) | 中间件安装与配置指南（ES IK / RabbitMQ DLX / 环境变量） |
| [docs/agent-design.md](agent-design.md) | Agent 设计方案：Harness 四层架构、工具、Playbook、自进化闭环 |
| [docs/backend-design.md](backend-design.md) | 本文件：Java 后端设计文档 |
| [docs/简历项目经历.md](简历项目经历.md) | 两个项目各 550 字简历版本（技术亮点 + 量化指标） |
| [docs/面试八股清单.md](面试八股清单.md) | 面试知识点与 T0/T1/T2 八股汇总 |
| [docs/interview-qa.md](interview-qa.md) | 项目答辩 Q&A |
| [docs/tech-transfer.md](tech-transfer.md) | 技术转让与交接文档 |
| [docs/perf-report.md](perf-report.md) | 性能压测报告 |
| [docs/agent2_test_report.md](agent2_test_report.md) | Agent2 功能测试报告 |

---

## 十二、未来扩展点（预留）

1. **ES 同步换成 Canal 订阅 Binlog**：增量同步已落地（§3.5：写事务投 `ES_SYNC` 事件 → Outbox 回查 MySQL 落索引 + `rebuild-index` 对账）。换 Canal 能再拿走两件事：① 业务代码里那次 `submitEsSync` 彻底消失，改库不再只有"走 IShopService"这一条路才同步（现在直连 SQL 改表仍然要等对账）；② 物理删行也能被捕获，不留孤儿文档。切换点只有一处：事件写入方由业务代码换成 Canal adapter，`syncShopById` 原样复用
2. **限流控制面**：动态规则已落地（§5.4：`/config` 写 Redis 规则键，`rate-limit.lua` 第 0 步读它，改阈值零重启、全部实例下一个请求起生效）。剩下的三件事都要靠外部组件，而且都不改判定逻辑：① **推送形态** —— 现在是人调 HTTP 写 Redis，接 Sentinel / Nacos 后换成规则中心推给一个「写 Redis 的人」，切换点就是 `DynamicConfig.put` 的调用方（Lua 侧一行不用动）；② **持久化与审计** —— 覆盖值只有 Redis 一份，`FLUSHALL` 或换实例就全丢，也没有「谁在什么时候改成了多少」的记录，配置中心天然带这两样；③ **多键原子性** —— Redis Cluster 下 `rate-limit.lua` 的 KEYS[1]（桶）与 KEYS[2]（规则）会落不同 slot 而报 CROSSSLOT，文档里记的解法是给同一个 api 的键加 hash tag，那要同时改 `RedisRateLimiter` 的键拼装和 `DynamicConfig.rateLimitRuleKey`
3. **Sentinel 替代自研熔断器**：自研三态熔断器满足单机需求，Sentinel 提供更丰富的规则配置 + Dashboard 可视化
4. **分库分表**：`tb_voucher_order` 按用户 ID 取模分表，RedisIdWorker 已支持分布式 ID
5. **支付风控**：接入第三方风控（设备指纹、行为分析），PaymentService.payOrder 前置风控检查
6. **秒杀预热**：秒杀活动开始前把库存预热到 Redis，避免活动开始瞬间 DB 压力
7. **接口幂等 token**：秒杀接口前置 `GET /voucher-order/token` 获取幂等 token，提交时校验，防重复提交
8. **链路追踪**：指标出口（`/actuator/prometheus`，§10.1）、本地采集栈（`observability/`：Prometheus + Grafana 面板 + 7 条告警规则）与单机 traceId 贯通（§10.3）已落地，切换点覆盖 HTTP / Redis Stream / RabbitMQ / 线程池 / `@Scheduled` 五类。剩下的都是"跨进程"那一档：① Agent 微服务是这套接口的调用方，它只要在请求里带上 `X-Trace-Id`，两边日志就能并到一条链上——机制在本仓库已经就绪，缺的是 Agent 侧那段改动的约定；② 采样、拓扑图、按服务聚合的 P99 需要 SkyWalking / Jaeger 这类专门组件，单靠 MDC 不再往上加
9. **秒杀落库耗时指标**：`dianping_seckill_precheck_total` 只计到"预检放行"，Stream 消费端没有埋耗时，所以"500 单约 4 秒落库"这个数至今只能靠轮询 DB 得到（`stress/verify.sh`）。补法是在 `SeckillVoucherListener` 上挂一个 `Timer`，并用消息里已有的 `createEpoch` 字段单独计"预订单在 Stream 里等了多久"——后者才是用户真正感知的延迟。这是当前观测面最大的一个缺口

---

*文档版本：v1.0（与代码状态 2026-08-11 对齐：双 Agent 微服务 / ES synonym_graph + IK 双 analyzer / @CircuitBreaker 三态熔断器 / PayLog 支付闭环 / 死信延迟取消 / Redis 8 种数据结构）*
