package com.hmdp.listener;

import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.hmdp.config.QueueConfig;
import com.hmdp.entity.TransactionOutbox;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.TransactionOutboxMapper;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.ConfirmedRabbitPublisher;
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
    private MeterRegistry meterRegistry;

    // ---- 指标快照：扫描线程写、Prometheus 抓取线程读 ----
    // 【为什么不让 Gauge 直接查库】抓取默认 15s 一次，写成"每次抓取 select count(*)"
    // 等于给一张正在出问题的表周期性加压；而且 DB 慢的时候抓取会超时堆积，
    // 监控比业务先倒下。所以 Gauge 只读内存里的这几个值。
    /** 上一轮取到的已到期待投递事件数（上限 BATCH_LIMIT） */
    private final AtomicLong pendingInLastBatch = new AtomicLong(0);
    /** 上一轮最老的已到期事件从落库起已等待的秒数；上一轮空批则为 0 */
    private final AtomicLong oldestReadyAgeSeconds = new AtomicLong(0);
    /** 上次扫描完成的 epoch 秒；用来发现 publisher 整个卡死 */
    private final AtomicLong lastScanEpochSec = new AtomicLong(0);

    @PostConstruct
    public void registerMeters() {
        meterRegistry.gauge("dianping.outbox.pending", pendingInLastBatch, v -> v.get());
        meterRegistry.gauge("dianping.outbox.oldest_age_seconds", oldestReadyAgeSeconds, v -> v.get());
        // 距上次扫描过了多久，抓取时现算。持续接近扫描间隔 = 正常；
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

    @Scheduled(fixedDelayString = "${transaction.outbox.publish-interval-ms:1000}")
    public void publishPendingEvents() {
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
        lastScanEpochSec.set(System.currentTimeMillis() / 1000);
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
