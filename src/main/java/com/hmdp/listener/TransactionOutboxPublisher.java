package com.hmdp.listener;

import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.hmdp.config.QueueConfig;
import com.hmdp.entity.TransactionOutbox;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.TransactionOutboxMapper;
import com.hmdp.service.IShopSearchService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.ConfirmedRabbitPublisher;
import com.hmdp.utils.DynamicConfig;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.TraceContext;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 本地事件表(Outbox)发布器 —— 【八股:Outbox模式解决什么问题?】
 *
 * 【八股:"DB事务成功"和"MQ发送成功"为什么无法原子?】
 * 方案A:事务里直接发MQ → 发送成功后事务回滚 = 幽灵消息(消费者处理了一个不存在的业务变更)
 * 方案B:事务提交后再发MQ → 提交后、发送前进程崩溃 = 消息永远丢失
 * 根因:数据库和MQ是两个系统,没有共享事务
 *
 * Outbox解法:业务事务里同库insert一条事件记录(与业务同事务,天然原子),
 * 后台任务扫表把事件真正发出去,发送成功才标记完成——最坏"重复发送",
 * 由消费端幂等兜底(至少一次语义)
 *
 * 【八股:为什么不用RocketMQ事务消息?】
 * 事务消息需要实现回查接口,broker与生产者强交互;Outbox只依赖本地DB+定时扫描,
 * 对MQ选型无侵入,还可顺便落审计。代价是扫描有延迟(本配置1秒)和事件表膨胀(需归档)
 */
@Slf4j
@Component
public class TransactionOutboxPublisher {

    public static final String PAY_NOTIFY = "PAY_NOTIFY";
    public static final String REFUND = "REFUND";
    public static final String REDIS_COMPENSATION = "REDIS_COMPENSATION";
    /**
     * ES 增量同步事件：payload 只有 {"shopId": n}，投递时回查 MySQL 再写索引。
     * 与 REDIS_COMPENSATION 一样走「不发 MQ、直接在扫描线程里做完」的内联分支，理由见 doPublish。
     */
    public static final String ES_SYNC = "ES_SYNC";

    /** 事件状态：0待发送 / 1已发送 / 2发送中 / 3死信（重试耗尽，见 markFailed） */
    private static final int STATUS_DEAD = 3;

    /**
     * 一条事件最多重试几次，超过即判定为死信，不再回到待发送队列。
     * 默认 20：退避到 300s 封顶后约合 70 分钟的重试预算 —— 足够覆盖 MQ/DB 的分钟级抖动，
     * 又不至于让一条永久失败的事件永远刷日志。
     */
    @Value("${transaction.outbox.max-retry:20}")
    private int maxRetry;

    /**
     * 扫描间隔的「默认值」，也就是没有动态覆盖时用的那一个。
     * 名字沿用改造前：它以前直接喂给 {@code @Scheduled} 当 fixedDelay，现在退化成
     * 动态配置的兜底值（真正的调度粒度是 {@link #SCAN_TICK_MS}）。
     */
    @Value("${transaction.outbox.publish-interval-ms:1000}")
    private long defaultIntervalMs;

    /**
     * 调度器 tick 的粒度（毫秒）。
     *
     * 【为什么不是「间隔直接做成 @Scheduled 的值」】
     * Spring 5.2（Boot 2.3）没有自适应 trigger，{@code fixedDelayString} 只在启动时解析一次，
     * 改配置必须重启 —— 而这正是这条改造要解决的。所以退而求其次：调度以最小粒度空转，
     * 每一轮开头再决定「到点了吗」。多出来的开销是一轮一次 Redis GET（见 {@link DynamicConfig}），
     * 换来的是积压时可以把间隔从 1s 立刻压到 200ms 追平，不用重启。
     *
     * 【下限为什么直接取 {@link DynamicConfig#MIN_INTERVAL_MS}】tick 比可调下限还大的话，
     * 那个下限就是个骗人的数字（配 50 实际跑 200）。直接引用同一个常量，
     * 两边永远不会漂移，也不需要「改一处忘改另一处」这种事发生。
     */
    private static final long SCAN_TICK_MS = DynamicConfig.MIN_INTERVAL_MS;

    /**
     * 上一轮真正扫描的时刻（epoch 毫秒），用来做间隔门控。
     * 只有调度线程写、也只有调度线程读，不需要 volatile。
     */
    private long lastRoundAtMs = 0L;

    /**
     * 每轮扫描的取数上限。
     * 【为什么要单独提出来】连续多轮"取满"就意味着投递速度追不上写入速度，
     * 这是积压的前兆——等表里堆几万条再发现就晚了。
     */
    private static final int BATCH_LIMIT = 50;

    @Resource
    private TransactionOutboxMapper outboxMapper;
    @Resource
    private ConfirmedRabbitPublisher confirmedRabbitPublisher;
    @Resource
    private IVoucherOrderService voucherOrderService;
    @Resource
    private IShopSearchService shopSearchService;
    @Resource
    private DynamicConfig dynamicConfig;
    @Resource
    private MeterRegistry meterRegistry;

    // ---- 指标快照：扫描线程写、Prometheus 抓取线程读 ----
    // 【为什么不让 Gauge 直接查库】抓取默认 15s 一次，写成"每次抓取 select count(*)"
    // 等于给一张正在出问题的表周期性加压；而且 DB 慢的时候抓取会超时堆积，
    // 监控比业务先倒下。所以 Gauge 只读内存里的这几个值。
    /** 上一轮取到的已到期待投递事件数（上限 BATCH_LIMIT） */
    private final AtomicLong pendingInLastBatch = new AtomicLong(0);
    /** 上一轮最老的已到期事件从落库起已等待的秒数；上一轮空批则为 0 */
    private final AtomicLong oldestReadyAgeSeconds = new AtomicLong(0);
    /** 上次调度 tick 的 epoch 秒；用来发现 publisher 整个卡死。注意它在门控之前更新，所以量的是「调度线程活着」，不是「上一轮真的扫了」 */
    private final AtomicLong lastScanEpochSec = new AtomicLong(0);

    @PostConstruct
    public void registerMeters() {
        meterRegistry.gauge("dianping.outbox.pending", pendingInLastBatch, v -> v.get());
        meterRegistry.gauge("dianping.outbox.oldest_age_seconds", oldestReadyAgeSeconds, v -> v.get());
        // 距上次调度 tick 过了多久，抓取时现算。正常应该恒等于毫秒级；
        // 这个数一直涨而 pending 也涨，说明线程根本没在跑（调度线程池饥饿：
        // 本项目的 @Scheduled 与 ES/Redis 补偿任务共用调度池），只看 pending 分不出来。
        meterRegistry.gauge("dianping.outbox.last_scan_age_seconds", lastScanEpochSec, v -> {
            long last = v.get();
            return last == 0 ? -1 : System.currentTimeMillis() / 1000 - last;
        });
    }

    /** 投递结果计数。type 取自事件的 eventType 字段（库里的值），为空时归到 unknown 而不是让标签炸掉 */
    private void countEvent(String type, String result) {
        meterRegistry.counter("dianping.outbox.event",
                "type", type == null ? "unknown" : type, "result", result).increment();
    }

    @Scheduled(fixedDelay = SCAN_TICK_MS)
    public void publishPendingEvents() {
        // 【心跳先于门控】这个指标回答的是「调度线程还在不在跑」，所以每次 tick 都要报活，
        // 不能因为这一轮被间隔门控挡掉就不报 —— 否则把间隔调大就会撞上 >30s 的「publisher 卡死」
        // 告警，一个故意的运维动作变成一场误报。真的卡死时（例如内联的 ES 调用不返回）
        // 下面的 tick 根本不会发生，告警照旧有效。
        lastScanEpochSec.set(System.currentTimeMillis() / 1000);

        if (!isRoundDue()) {
            return;
        }

        // 0. 自愈:卡在"发送中(status=2)"超过1分钟的事件重置回待发送
        //    (发送中途崩溃的实例永远不会回来标记它,必须有超时回收,类似分布式锁的TTL兜底)
        int recovered = outboxMapper.update(null, new UpdateWrapper<TransactionOutbox>()
                .set("status", 0)
                .set("next_retry_time", LocalDateTime.now())
                .set("update_time", LocalDateTime.now())
                .eq("status", 2)
                .lt("update_time", LocalDateTime.now().minusMinutes(1)));
        if (recovered > 0) {
            // 每回收一条 = 有实例在"已抢占、未投递"之间死掉过。偶发正常（发布重启），
            // 持续出现就是实例被强杀或发送环节卡死超过 1 分钟。
            meterRegistry.counter("dianping.outbox.stuck_recovered").increment(recovered);
        }
        List<TransactionOutbox> events = outboxMapper.selectList(new QueryWrapper<TransactionOutbox>()
                .eq("status", 0)
                .le("next_retry_time", LocalDateTime.now())
                .orderByAsc("id")
                .last("LIMIT " + BATCH_LIMIT));
        snapshotBacklog(events);
        for (TransactionOutbox event : events) {
            // 【八股:CAS抢占防多实例重复发】update ... set status=2 where id=? and status=0
            // 只有一个实例能更新成功(affected=1),其余实例抢不到直接跳过
            // 这与秒杀"条件更新订单状态"是同一个数据库CAS思想,无需引入分布式锁
            boolean claimed = outboxMapper.update(null, new UpdateWrapper<TransactionOutbox>()
                    .set("status", 2)
                    .set("update_time", LocalDateTime.now())
                    .eq("id", event.getId())
                    .eq("status", 0)) == 1;
            if (claimed) {
                event.setStatus(2);
                publish(event);
            } else {
                // 多实例下的正常竞争，不是错误；但要能数出实例之间在多大程度上互相抢
                countEvent(event.getEventType(), "claim_lost");
            }
        }
    }

    /**
     * 按当前生效的间隔判断这一轮该不该真的扫。
     *
     * 【间隔从哪来】Redis 里的 {@link RedisConstants#OUTBOX_PUBLISH_INTERVAL_KEY}，
     * 没配 / 配了非法值 / Redis 读不到 → 回退到 {@code transaction.outbox.publish-interval-ms}，
     * 也就是没做这次改造之前的行为。
     *
     * 【多实例】所有实例读同一个键，所以间隔是一致的；即便有个位数毫秒的偏差也无所谓，
     * 真正防重复投递的是下面的 CAS 抢占，不是间隔对齐。
     */
    private boolean isRoundDue() {
        long intervalMs = effectiveIntervalMs();
        long now = System.currentTimeMillis();
        // lastRoundAtMs=0（首轮）必然到期，不需要额外的"第一次"分支
        if (now - lastRoundAtMs < intervalMs) {
            return false;
        }
        lastRoundAtMs = now;
        return true;
    }

    /**
     * 当前真正生效的扫描间隔，供 {@code GET /config} 回显。
     *
     * 【为什么暴露这个方法而不是让管理接口自己再算一遍】
     * 「生效值」的口径（读哪个键、越界怎么办、Redis 读不到怎么办）只能有一处定义。
     * 管理接口另算一遍的话，它报出来的数可能和实际跑的不一样，
     * 而这类接口存在的唯一理由就是让人相信它报的数。
     * 注意：本方法不推进 {@link #lastRoundAtMs}，读它不会打断节奏。
     */
    public long effectiveIntervalMs() {
        return dynamicConfig.getLong(RedisConstants.OUTBOX_PUBLISH_INTERVAL_KEY,
                defaultIntervalMs, DynamicConfig.MIN_INTERVAL_MS, DynamicConfig.MAX_INTERVAL_MS);
    }

    /**
     * 把这一轮的积压情况记进内存快照。
     *
     * 【为什么"最老等待时长"能免费拿到】查询是 orderByAsc("id") 且 id 自增，
     * 所以批里第一条就是最老的待投递事件——不必为这个指标多发一条 SQL。
     * 口径要注意：处于指数退避中（next_retry_time 在未来）的事件不在批里，
     * 所以这个数反映"已到期待投递的最老事件等了多久"，不是全表最老记录。
     */
    private void snapshotBacklog(List<TransactionOutbox> events) {
        pendingInLastBatch.set(events.size());
        TransactionOutbox oldest = events.isEmpty() ? null : events.get(0);
        if (oldest == null || oldest.getCreateTime() == null) {
            oldestReadyAgeSeconds.set(0);
            return;
        }
        oldestReadyAgeSeconds.set(Duration.between(oldest.getCreateTime(), LocalDateTime.now()).getSeconds());
    }

    private void publish(TransactionOutbox event) {
        // 【链路追踪】@Scheduled 线程没有上游请求可接，所以每条事件起一个自己的 id：
        // 这条事件在本次投递里的所有日志（发送/标记/失败原因）共用它，消费者那端还能靠消息头续上。
        //
        // 【为什么这一跳不接原始 traceId】写事件的是业务线程、投递的是扫描线程，中间隔了一次落表，
        // 而表里没有 trace 列（加列要走 DDL，项目目前没有迁移工具）。要硬接只能在两端各打一行带
        // outbox id 的日志去对，等于给这条链路凭空加一倍日志量。跨这一跳请直接用 payload 里的
        // orderId / tradeNo grep —— 那本来就是业务主键，比 traceId 更好用。
        try (TraceContext.Scope ignored = TraceContext.enter()) {
            doPublish(event);
        }
    }

    private void doPublish(TransactionOutbox event) {
        try {
            if (REDIS_COMPENSATION.equals(event.getEventType())) {
                VoucherOrder order = JSONUtil.toBean(event.getPayload(), VoucherOrder.class);
                if (order.getId() == null || order.getUserId() == null
                        || order.getVoucherId() == null) {
                    throw new IllegalArgumentException("Redis补偿事件字段不完整");
                }
                voucherOrderService.releaseRejectedReservation(order, true, true);
                markSent(event);
                return;
            }
            // 【为什么 ES 同步也走内联分支，不投 MQ】
            // 1. 投给谁？消费者要做的事和这里一模一样（回查 MySQL + 写一条文档），
            //    中间那跳队列不提供任何东西：不能批量、不需要削峰（一轮扫描最多 50 条）。
            // 2. 反过来还更糟：本项目的队列都没配 DLX，@RabbitListener 里抛异常 = 消息 requeue，
            //    ES 一直不通就是热循环刷日志；而走 outbox 天然拿到指数退避 + 死信落表，
            //    和 REDIS_COMPENSATION 是同一套兜底。
            // 3. 代价（已知局限）：ES「慢而不通」时会拖住这条共用的扫描线程，
            //    PAY_NOTIFY 的投递延迟跟着一起涨。要隔离就给 ES_SYNC 单开一个调度池，
            //    现在这个量级（商铺 <1000、改动极少）不值得。
            if (ES_SYNC.equals(event.getEventType())) {
                Long shopId = JSONUtil.parseObj(event.getPayload()).getLong("shopId");
                if (shopId == null) {
                    throw new IllegalArgumentException("ES同步事件缺少 shopId");
                }
                // 这里不 catch：写不进 ES 就抛，交给 markFailed 退避重试
                shopSearchService.syncShopById(shopId);
                markSent(event);
                return;
            }
            String exchange;
            String routingKey;
            if (PAY_NOTIFY.equals(event.getEventType())) {
                exchange = QueueConfig.PAY_NOTIFY_EXCHANGE;
                routingKey = QueueConfig.PAY_NOTIFY_ROUTING_KEY;
            } else if (REFUND.equals(event.getEventType())) {
                exchange = QueueConfig.REFUND_EXCHANGE;
                routingKey = QueueConfig.REFUND_ROUTING_KEY;
            } else {
                markFailed(event, "未知事件类型");
                return;
            }
            confirmedRabbitPublisher.send(exchange, routingKey, event.getPayload(),
                    "outbox:" + event.getId() + ":" + event.getRetryCount());
            markSent(event);
        } catch (Exception e) {
            markFailed(event, e.getMessage());
        }
    }

    private void markSent(TransactionOutbox event) {
        outboxMapper.update(null, new UpdateWrapper<TransactionOutbox>()
                .set("status", 1)
                .set("update_time", LocalDateTime.now())
                .eq("id", event.getId())
                .eq("status", 2));
        countEvent(event.getEventType(), "sent");
    }

    private void markFailed(TransactionOutbox event, String reason) {
        // 【八股:指数退避重试】delay = min(2^retry, 300s):
        // 每失败一次等待翻倍,避免下游故障时被固定频率重试打死;封顶防止无限增长
        // (与Agent侧LLM重试、Redisson锁重试是同一思想)
        int retry = event.getRetryCount() == null ? 1 : event.getRetryCount() + 1;
        long delaySeconds = Math.min(300, 1L << Math.min(retry, 8));
        // 【退避封顶之后必须有终点】退避到 300s 封顶意味着失败次数不再受总时长约束,
        // 没有上限就是一条永久失败的事件每 5 分钟重试一次、重试到天荒地老。
        // 实测就撞上过一条 retry=2357 的 REDIS_COMPENSATION（对应的秒杀券早已不存在,
        // 属于永远不可能成功的事件）：它不报错、不影响业务，只是每轮扫描白占一次行锁。
        boolean dead = retry >= maxRetry;
        outboxMapper.update(null, new UpdateWrapper<TransactionOutbox>()
                .set("retry_count", retry)
                .set("status", dead ? STATUS_DEAD : 0)
                .set("next_retry_time", LocalDateTime.now().plusSeconds(delaySeconds))
                .set("update_time", LocalDateTime.now())
                .eq("id", event.getId())
                .eq("status", 2));
        countEvent(event.getEventType(), dead ? "dead" : "failed");
        if (dead) {
            // 死信不自动重试，但整行留在表里（含 payload）供人工核对后改回 status=0 重放。
            // 失败原因只有日志里有：表没有 error 列，加列要走 DDL，先不做。
            log.error("Outbox事件超过最大重试次数{}，判定为死信(status={})，需人工处理: id={}, type={}, retry={}, reason={}",
                    maxRetry, STATUS_DEAD, event.getId(), event.getEventType(), retry, reason);
            return;
        }
        log.warn("Outbox事件发布失败, id={}, type={}, retry={}, reason={}",
                event.getId(), event.getEventType(), retry, reason);
    }
}
