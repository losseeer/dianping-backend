package com.hmdp.listener;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.TraceContext;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.ByteRecord;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Set;

/**
 * 秒杀预占对账 —— 【八股:对账(reconciliation)是分布式系统里的最后一张网】
 *
 * 【八股:为什么前面已经有四层可靠性了,还需要它?】
 * {@link SeckillVoucherListener} 的四层（PENDING 重读 / XCLAIM 认领 / 消费端幂等 /
 * 毒丸终点）全部建立在一个前提上：<strong>消息还在 Stream 或 PENDING 里</strong>。
 * 一旦这个前提不成立，Redis 侧的预扣就成了孤儿，谁也发现不了：
 * <ul>
 *   <li>Redis 丢数据（节点崩溃 + AOF 未刷盘），扣减与消息一起消失；</li>
 *   <li>消息被丢弃而预扣没被补偿（{@code IllegalArgumentException} 那条分支——字段都解不开了，
 *       连 orderId 都取不到，就地回滚是不可能的）；</li>
 *   <li>消息被人工 XDEL / 消费者组被重建 / PEL 未被持久化。</li>
 * </ul>
 * 这些情况下：<strong>库存永久少 1、该用户的一人一单资格永久被占，延迟取消消息也永远不会发出</strong>
 * （它是落库成功之后才发的，压根没走到那一步）。表现就是"这张券少卖了、这个用户再也抢不到"，
 * 而日志里一片安静。对账要回答的就是这一个问题：Redis 里有没有扣了库存却永远不会有订单的残留。
 *
 * 【八股:为什么预扣不能靠"同库同事务"来解决,非要另开一张网?】
 * Outbox 模式（见 {@link TransactionOutboxPublisher}）能保证"业务变更与消息"的原子性，
 * 前提是两者在<strong>同一个数据库事务</strong>里。秒杀的预扣在 Redis、订单在 MySQL，
 * 两者没有共享事务——Lua 已经把"扣减 + 发消息"做到了 Redis 内部的原子，
 * 但"Redis 的预扣"与"MySQL 的订单"之间那道缝，任何事务机制都缝不上，只能对账。
 *
 * 【八股:怎么保证对账自己不会误伤?】
 * 判据是两条同时成立，缺一不可：
 * <ol>
 *   <li><b>DB 里没有这个订单</b>（有 = 正常落库，直接清理索引条目收工）；</li>
 *   <li><b>消息也已经不在 Stream 里了</b>（还在 = 消费者只是慢，绝不能回滚）。
 *       这条是关键：只按"时间久了还没落库"回滚，会把一条排队中的消息的库存
 *       提前还回去，等它稍后被消费成功，就等于同一份库存被卖了两次。</li>
 * </ol>
 * 再加上第三重保险：真正的回滚动作走 {@code restore-seckill.lua}，其中
 * {@code seckill:restored:{orderId}} 的 {@code SET NX} 保证每个订单号一辈子只回滚一次。
 * 于是即便前两条判据在极端时序下都被绕过，库存也不会被重复加回去。
 *
 * 【八股:多实例同时跑会不会重复回滚?】
 * 会同时跑（没有分布式锁，也没有 CAS 抢占），但重复是无害的：ZREM 是幂等的，
 * 回滚本身有上面那道 NX 锁。这与 Outbox 那边必须用 CAS 抢占有本质区别——
 * 那边重复的后果是"多发一条 MQ 消息"，这里重复的后果是"少做一次无意义的事"。
 *
 * 【已知局限】
 * 索引键自身有 {@link RedisConstants#SECKILL_PENDING_INDEX_TTL} 的 TTL(2小时)兜底，
 * 且它只在"有新预扣写入"时被刷新。所以若某张券长期无人下单、索引整个过期，
 * 期间残留的孤儿预扣就再也扫不到了——这是拿内存占用换来的边界，
 * 取舍是：宁可漏掉"闲置两小时以上的冷券"，也不让索引无限增长。
 */
@Slf4j
@Component
public class SeckillReservationReconciler {

    /**
     * 对账窗口（秒）：只看"预扣已发生超过这么久"的登记。
     *
     * 【为什么不能更小】窗口内本来就是"正常在途"的区间：消息可能还在 PENDING 里重试、
     * 消费者可能正在落库。窗口取小了，就会去回滚一批马上要成功的预占。
     * 默认 15 分钟 = 预订单可见窗口(10分钟) + 消费端重试预算(约40秒) + 余量，
     * 也就是"用户那边早已看不到这笔订单了"之后才开始动手。
     */
    @Value("${seckill.reconcile.horizon-seconds:900}")
    private long horizonSeconds;

    /** 单轮最多处理多少条。对账是低频兜底，积压几万条说明出的是别的问题，靠它救不回来 */
    @Value("${seckill.reconcile.batch-size:100}")
    private int batchSize;

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IVoucherOrderService voucherOrderService;
    @Resource
    private MeterRegistry meterRegistry;

    /**
     * 窗口必须小于索引 TTL，否则"等到能对账时登记已经过期了"——这个错误会静默地
     * 让整个对账任务变成空转（永远扫不到东西，也不报错），所以在启动时就说一声。
     * 只告警不阻断启动：对账是兜底路径，为一个兜底组件的配置问题拦下整个应用不值得。
     */
    @PostConstruct
    void warnOnInvertedConfig() {
        if (horizonSeconds >= RedisConstants.SECKILL_PENDING_INDEX_TTL) {
            log.error("对账窗口({}s)不小于预扣索引TTL({}s)：登记会在等到对账之前先过期，"
                            + "对账任务将形同空转，请调小 seckill.reconcile.horizon-seconds",
                    horizonSeconds, RedisConstants.SECKILL_PENDING_INDEX_TTL);
        }
    }

    /**
     * 定时对账：把"扣了 Redis 库存、却确定不会有订单"的残留找出来回滚。
     *
     * 【为什么用 @Scheduled 而不是像 Outbox 那样做运行期可调间隔】Outbox 需要的是
     * "积压时把间隔从 1s 压到 200ms 追平"，那是个必须立刻生效的运维动作；
     * 对账是低频兜底，五分钟还是十分钟扫一轮没有实质区别，不值得为它多开一个配置项
     * 和维护它的管理接口。真实生效的差异只会体现在"故障被发现的延迟"上，
     * 而那有指标看得见（见 reconcile 末尾的计数）。
     *
     * 【为什么初始延迟不是 0】应用刚起来时消费线程可能还在预热/追赶积压，
     * 立刻对账等于在系统最忙、最可能误判的时候动手。等一轮启动抖动过去更稳妥。
     */
    @Scheduled(fixedDelayString = "${seckill.reconcile.interval-ms:300000}",
            initialDelayString = "${seckill.reconcile.initial-delay-ms:60000}")
    public void reconcile() {
        long cutoffEpochSecond = Instant.now().getEpochSecond() - horizonSeconds;
        Set<String> candidates = stringRedisTemplate.opsForZSet().rangeByScore(
                RedisConstants.SECKILL_PENDING_INDEX_KEY,
                Double.NEGATIVE_INFINITY, cutoffEpochSecond, 0, batchSize);
        if (candidates == null || candidates.isEmpty()) {
            return;
        }
        int confirmed = 0;
        int inFlight = 0;
        int reverted = 0;
        int failed = 0;
        for (String candidate : candidates) {
            ReconcileOutcome outcome;
            try {
                outcome = check(candidate);
            } catch (RuntimeException e) {
                // 不 ZREM：本轮读库/读 Stream 失败不能当成"确认过了"，留着下一轮再看
                outcome = ReconcileOutcome.FAILED;
                log.warn("秒杀预占对账异常，本轮跳过该条: member={}", candidate, e);
            }
            // 计数只在这一处：check 内部各分支自己再记一遍的话，
            // 异常这条路径必然被漏掉（它不在 check 的任何一个 return 里）
            count(outcome.tag);
            switch (outcome) {
                case CONFIRMED:
                    confirmed++;
                    break;
                case IN_FLIGHT:
                    inFlight++;
                    break;
                case REVERTED:
                    reverted++;
                    break;
                default:
                    failed++;
                    break;
            }
        }
        if (reverted > 0 || failed > 0) {
            log.info("秒杀预占对账完成: 候选={}, 已落库={}, 仍在途={}, 已回滚={}, 失败={}",
                    candidates.size(), confirmed, inFlight, reverted, failed);
        }
    }

    /**
     * 判定单条登记，并在需要时执行回滚并清理索引。
     *
     * 【索引条目什么时候可以被摘掉】只有"这条登记已经结清"才 ZREM：
     * CONFIRMED（订单在）与 REVERTED（已回滚）都要摘；MALFORMED 摘掉是因为它永远
     * 结不清、留着只是每轮白占一个名额。
     * IN_FLIGHT 与 FAILED 绝不能摘——前者还要靠它下一轮接着看，后者是这一轮没看成。
     */
    private ReconcileOutcome check(String candidate) {
        Reservation reservation = Reservation.parse(candidate);
        if (reservation == null) {
            // 拼不出来的脏数据（人工误写、旧版本格式）留着只会每轮白占一个名额
            log.error("预扣索引条目格式非法，直接清理: member={}", candidate);
            removeIndexEntry(candidate);
            return ReconcileOutcome.MALFORMED;
        }
        if (voucherOrderService.getById(reservation.orderId) != null) {
            // 正常路径：订单已落库，这条登记可以结清了。
            // 不打日志——这是绝大多数情况，每轮刷一遍等于把日志淹掉。
            removeIndexEntry(candidate);
            return ReconcileOutcome.CONFIRMED;
        }
        if (streamEntryExists(reservation.streamEntryId)) {
            // 消息还在 Stream 里 = 消费者只是慢（正在重试、或实例停摆期间积压），
            // 这笔预占是活的：现在回滚，等它稍后被消费成功就是同一份库存卖了两次。
            return ReconcileOutcome.IN_FLIGHT;
        }
        revert(reservation);
        removeIndexEntry(candidate);
        return ReconcileOutcome.REVERTED;
    }

    private void removeIndexEntry(String member) {
        stringRedisTemplate.opsForZSet().remove(RedisConstants.SECKILL_PENDING_INDEX_KEY, member);
    }

    /**
     * 回滚预占：既还库存，也还"一人一单"资格。
     *
     * 【为什么两个都还】这条预占确定不会产生订单了，那它占用的两样东西都该释放：
     * 库存不还是少卖一张，资格不还是这个用户被永久挡住——而后者用户是能感知到的
     * （同一个券永远提示"不能重复下单"），比少卖一张更糟。
     */
    private void revert(Reservation reservation) {
        VoucherOrder order = new VoucherOrder();
        order.setId(reservation.orderId);
        order.setUserId(reservation.userId);
        order.setVoucherId(reservation.voucherId);
        try (TraceContext.Scope ignored = TraceContext.enter()) {
            log.error("发现无主预占（Redis已扣库存但确定不会有订单），回滚: orderId={}, userId={}, voucherId={}",
                    reservation.orderId, reservation.userId, reservation.voucherId);
            voucherOrderService.releaseRejectedReservation(order, true, true);
        }
    }

    /** 消息是否还在 Stream 里（XRANGE 单条精确查，比 XLEN 之类的近似判断更直接） */
    private boolean streamEntryExists(String entryId) {
        List<ByteRecord> found = stringRedisTemplate.execute((RedisCallback<List<ByteRecord>>) connection ->
                connection.xRange(RedisConstants.SECKILL_ORDER_STREAM_KEY.getBytes(StandardCharsets.UTF_8),
                        Range.closed(entryId, entryId)));
        return found != null && !found.isEmpty();
    }

    private void count(String result) {
        meterRegistry.counter("dianping.seckill.reconcile", "result", result).increment();
    }

    /** 判定结果，同时充当指标标签：两者必须是同一份定义，否则看板和代码会各说各话 */
    private enum ReconcileOutcome {
        /** DB 里已有订单：正常落库，索引条目结清 */
        CONFIRMED("confirmed"),
        /** 消息还在 Stream 里：消费者只是慢，这笔预占是活的 */
        IN_FLIGHT("in_flight"),
        /** 确定不会有订单：已回滚库存与一人一单资格 */
        REVERTED("reverted"),
        /** 本轮读库/读 Stream 失败：条目保留，下一轮再看 */
        FAILED("failed"),
        /** 索引 member 拼不出订单信息：脏数据，清掉且不碰业务 */
        MALFORMED("malformed");

        private final String tag;

        ReconcileOutcome(String tag) {
            this.tag = tag;
        }
    }

    /** 索引 member 的解析结果：{orderId}:{userId}:{voucherId}:{streamEntryId}，见 seckill.lua 3.9 */
    private static final class Reservation {
        private final long orderId;
        private final long userId;
        private final long voucherId;
        private final String streamEntryId;

        private Reservation(long orderId, long userId, long voucherId, String streamEntryId) {
            this.orderId = orderId;
            this.userId = userId;
            this.voucherId = voucherId;
            this.streamEntryId = streamEntryId;
        }

        /** 任何一段缺失或不是数字都返回 null（调用方按脏数据清理），不让对账因为一条坏数据整轮失败 */
        private static Reservation parse(String member) {
            if (member == null) {
                return null;
            }
            String[] parts = member.split(":");
            if (parts.length != 4) {
                return null;
            }
            try {
                return new Reservation(Long.parseLong(parts[0]), Long.parseLong(parts[1]),
                        Long.parseLong(parts[2]), parts[3]);
            } catch (NumberFormatException e) {
                return null;
            }
        }
    }
}
