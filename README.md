# dianping-backend

基于 Spring Boot 2.3 的高并发本地生活服务后端（点评 / 商铺 / 优惠券秒杀 / 订单支付 / 全文搜索）。在经典点评项目业务骨架之上，重点补齐了**生产级一致性设计**：数据库部分唯一索引兜底、Outbox 模式、订单状态机、缓存三防、搜索熔断降级。

## 技术栈

| 层 | 选型 |
|---|---|
| 框架 | Spring Boot 2.3.12 / MyBatis-Plus / JDK 8 |
| 存储 | MySQL 8（唯一约束兜底 + 条件更新乐观锁）/ Redis（缓存 / 锁 / 全局 ID / Stream） |
| 消息 | RabbitMQ（延迟队列 / 通知 / Outbox 发布）/ Redis Stream（秒杀异步落库） |
| 搜索 | Elasticsearch（IK 分词 + synonym_graph 同义词扩展） |
| 可观测 | Micrometer + Prometheus 端点（缓存命中 / 限流判定 / 熔断状态跃迁 / Outbox 积压 / 服务端 P99 直方图）；MDC traceId 贯通 HTTP → Stream → MQ → 线程池 |
| 压测 | JMeter（`stress/` 含场景脚本、数据准备与校验脚本） |

## 核心设计

1. **缓存三防**：空值缓存防穿透、随机 TTL 防雪崩、逻辑过期 + 互斥重建防击穿（`CacheClient`）
2. **秒杀链路**：Lua 原子预检（库存 + 一人一单）→ Redis Stream 异步落库（消费者组 ACK/XCLAIM 故障转移）→ 死信队列 30 分钟超时取消
3. **订单状态机 + 支付闭环**：`UNPAID→PAID→VERIFIED / REFUNDING→REFUNDED` 六态状态机；取消与支付竞态由条件 UPDATE 裁决；`tb_pay_log` 流水幂等 + 迟到支付自动原路退款
4. **DB 兜底约束**：`uk_active_user_voucher` / `uk_pending_order` 终态置 NULL 的部分唯一索引，从存储层杜绝超卖与重复支付
5. **Outbox 模式**：支付通知、退款、Redis 补偿等跨链路消息先随业务事务落库，再由 publisher 轮询投递，保证事务与消息最终一致；退避重试封顶后仍失败的事件超过 `max-retry`（默认 20 次 ≈ 70 分钟）判定为死信（`status=3`）停止重试，整行留在表里供人工核对重放
6. **搜索熔断降级**：ES 检索失败时熔断打开并回退 MySQL LIKE 兜底；`synonym_graph` 放在 search analyzer 侧，同义词热更新无需重建索引

## 快速开始

```bash
# 1. 初始化数据库（MySQL 8）
mysql -u root -p -e "CREATE DATABASE dingping"
mysql -u root -p dingping < sql/schema/001_core.sql
mysql -u root -p dingping < sql/schema/002_payment_and_search.sql
mysql -u root -p dingping < sql/data/001_test_data.sql   # 120 商铺 + 1005 用户测试数据

# 2. 启动中间件：Redis(6379) / RabbitMQ(5672) / Elasticsearch(9200，需安装 IK 分词插件)

# 3. 启动（JDK 8）
mvn spring-boot:run   # 默认端口 8081
```

详细部署与排障见 `docs/SETUP.md`，压测方法与结论见 `docs/perf-report.md`。

## 文档索引

| 文档 | 内容 |
|---|---|
| `docs/SETUP.md` | 环境搭建与部署手册 |
| `docs/backend-design.md` | 后端设计说明 |
| `docs/perf-report.md` | 秒杀链路四阶段压测报告 |
| `docs/tech-transfer.md` | 可迁移工程模式与落地指南 |
| `docs/orginal_README.md` | 原始项目说明 |

## 关联项目

配套的双 Agent 智能服务（评价摘要 / 商铺推荐）与 Vue 3 前端为独立仓库。
