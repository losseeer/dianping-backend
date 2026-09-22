package com.hmdp.listener;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.config.QueueConfig;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.enums.OrderCreationResult;
import com.hmdp.enums.OrderStatus;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.ConfirmedRabbitPublisher;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.TraceContext;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.ByteRecord;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

/**
 * 秒杀订单Stream消费者 —— 【八股:Redis Stream 消费者组与消息可靠性】
 *
 * 【八股:为什么削峰选Redis Stream而不是RabbitMQ?】
 * - 秒杀预扣本来就在Redis里完成,Lua脚本内直接XADD,一次网络往返零跨系统窗口
 * - Stream自带消费者组语义:XREADGROUP/XACK/PENDING/XCLAIM,轻量且故障转移能力强
 * - RabbitMQ留给延迟取消场景(需要成熟TTL+死信),各取所长
 *
 * 【八股:消息不丢的三层可靠性(本类的核心设计)】
 * 1. 正常路径:处理成功才XACK+XDEL,处理异常保留在PENDING列表
 * 2. 自愈路径:每轮循环重读自己的PENDING(processOwnPending)——覆盖"处理中崩溃"的消息
 * 3. 故障转移:XCLAIM认领其他崩溃实例遗留的消息(claimAbandonedPending,
 *    minIdle=30s保证不抢正在处理的消息)——这是Stream相对普通队列的关键优势
 * 4. 毒丸终点:投递次数到顶(MAX_DELIVERY_COUNT)即移出循环并回滚预占(deadLetter)——
 *    前三层保证"消息不会丢",但一层都不管"消息永远处理不成功"该怎么办
 *
 * 【八股:这四层仍然覆盖不到的那一段由谁兜】
 * 它们全都建立在"消息还在Stream/PENDING里"之上。Redis 丢数据、消息被手工XDEL、
 * 订单事件被丢弃(IllegalArgumentException)这些情况下,预扣会永远留在Redis上:
 * 库存少1、用户的"一人一单"资格被永久占用,而延迟取消消息根本不会发出(它是在
 * 落库成功之后才发的)。所以另有一个跨订单维度的对账任务在这个类之外收口,
 * 见 {@link SeckillReservationReconciler} 与 seckill.lua 3.9 的全局预扣索引。
 *
 * 【八股:为什么单线程消费?】
 * newSingleThreadExecutor:①同一券的库存扣减天然串行,避免并发落库冲突
 * ②消费顺序可控;吞吐不够时按voucherId分片到多个组/实例水平扩展(daemon线程不阻塞JVM退出)
 */
@Slf4j
@Component
public class SeckillVoucherListener {

    private static final int PENDING_BATCH_SIZE = 20;
    private static final int MAX_PENDING_SCAN = 200;
    /** 只认领空闲超过该时长的消息：避免抢走其他实例正在处理中的消息（处理慢≠崩溃） */
    private static final Duration IDLE_CLAIM_THRESHOLD = Duration.ofSeconds(30);

    /**
     * 自己的 PENDING 重扫间隔。
     *
     * 【为什么不能每轮都扫】旧实现每轮循环（主读阻塞2秒后）都重读一遍自己的 PENDING，
     * 一条失败的订单就是每2秒重试一次。频率高不代表恢复快——失败原因通常是
     * DB/Redis/Redisson 级别的抖动，2秒和5秒的恢复速度没有区别，但重试次数直接决定了
     * "多久之后会被判定成毒丸"（见 MAX_DELIVERY_COUNT），节奏太快等于把预算烧得太快。
     */
    private static final long OWN_PENDING_RETRY_INTERVAL_MS = 5000L;

    /**
     * 单条消息最多投递几次，到顶即判定为毒丸（见 {@link #deadLetter}）。
     *
     * 【为什么是120】重试预算 = {@link #OWN_PENDING_RETRY_INTERVAL_MS} × 这个数 ≈ 10 分钟，
     * 刻意与预订单的可见窗口（{@code SECKILL_PENDING_ORDER_TTL}）对齐，理由是：
     * 用户能看见"订单创建中"的整个窗口里，消息都还在重试；窗口一关，这笔预占对用户
     * 已经不存在了，就不该再占着库存和一人一单资格。反过来，若把预算压到几十秒，
     * 一次 MySQL 主从切换就能让一批本该成功的订单被误判成毒丸并回滚掉。
     * 改 {@link #OWN_PENDING_RETRY_INTERVAL_MS} 或预订单 TTL 时，这里要跟着重算。
     */
    private static final long MAX_DELIVERY_COUNT = 120L;

    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r, "seckill-order-consumer");
        thread.setDaemon(true);
        return thread;
    });
    private final String consumerName = "order-" + ManagementFactory
            .getRuntimeMXBean().getName().replace('@', '-');
    private volatile boolean running = true;
    /** 两条 PENDING 扫描各自的翻页游标；null = 从头开始（不是 "0"，那样会漏掉第一条的边界） */
    private String ownPendingCursor;
    private String abandonedPendingCursor;
    /** 上次重扫自己 PENDING 的时刻（epoch毫秒）。只有消费线程读写，不需要 volatile */
    private long lastOwnPendingScanAtMs;

    @Resource
    private IVoucherOrderService voucherOrderService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private ConfirmedRabbitPublisher confirmedRabbitPublisher;
    @Resource
    private MeterRegistry meterRegistry;

    @PostConstruct
    public void start() {
        createConsumerGroup();
        executor.submit(this::consumeLoop);
    }

    @PreDestroy
    public void stop() {
        running = false;
        executor.shutdownNow();
    }

    private void createConsumerGroup() {
        try {
            stringRedisTemplate.execute((RedisCallback<Object>) connection -> connection.execute(
                    "XGROUP",
                    "CREATE".getBytes(),
                    RedisConstants.SECKILL_ORDER_STREAM_KEY.getBytes(),
                    RedisConstants.SECKILL_ORDER_STREAM_GROUP.getBytes(),
                    "0".getBytes(),
                    "MKSTREAM".getBytes()
            ));
        } catch (Exception e) {
            if (e.getMessage() == null || !e.getMessage().contains("BUSYGROUP")) {
                throw e;
            }
        }
    }

    private void consumeLoop() {
        // 【八股:XREADGROUP语义】COUNT 10每轮最多拉10条;BLOCK 2000没有新消息时阻塞2秒
        // (阻塞读优于忙轮询:空转不烧CPU);ReadOffset.lastConsumed()从组内未消费处继续
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                List<MapRecord<String, Object, Object>> records = stringRedisTemplate.opsForStream().read(
                        Consumer.from(RedisConstants.SECKILL_ORDER_STREAM_GROUP, consumerName),
                        StreamReadOptions.empty().count(10).block(Duration.ofSeconds(2)),
                        StreamOffset.create(RedisConstants.SECKILL_ORDER_STREAM_KEY, ReadOffset.lastConsumed())
                );
                // 新投递的消息必然是第1次投递，投递次数表留空 = 不参与毒丸判定
                processRecords(records, Collections.emptyMap());
                processOwnPending();
                claimAbandonedPending();
            } catch (Exception e) {
                if (running) {
                    log.error("消费秒杀订单Stream失败, consumer={}", consumerName, e);
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    /**
     * 重读自己的 PENDING —— 覆盖"读到消息之后、XACK 之前崩掉/失败"的消息。
     *
     * 【八股:为什么从 XREADGROUP 换成 XPENDING+XCLAIM】
     * 关键差别只有一点：<strong>投递次数</strong>。用 XREADGROUP 重读自己的历史
     * (ReadOffset.from(cursor)) 返回的是 PEL 里的条目，但不会累加"投递次数"，
     * 于是这个计数永远停在首次投递时的 1 —— "这条消息已经重试了多少次"在旧路径上
     * 根本无法观测，一条必然失败的消息可以在这里安静地转到天荒地老。
     * XPENDING 会带出投递次数，XCLAIM 到自己是把消息取回来的语义最直接的写法
     * （顺带把计数+1，正好等于"这一次也算一次投递"）。
     * 代价是每批多一次往返，且只有在真有 PENDING 时才走到（空批一次往返即返回）。
     */
    private void processOwnPending() {
        // 节流：失败的订单不必每轮（约2秒）都重试一次，见 OWN_PENDING_RETRY_INTERVAL_MS。
        // 首轮 lastOwnPendingScanAtMs=0，必然放行。
        long now = System.currentTimeMillis();
        if (now - lastOwnPendingScanAtMs < OWN_PENDING_RETRY_INTERVAL_MS) {
            return;
        }
        lastOwnPendingScanAtMs = now;
        int scanned = 0;
        while (scanned < MAX_PENDING_SCAN) {
            PendingBatch batch = readOwnPendingBatch();
            if (batch == null || batch.scanned == 0) {
                ownPendingCursor = null;
                return;
            }
            processRecords(batch.records, batch.deliveryCounts);
            scanned += batch.scanned;
            ownPendingCursor = batch.lastId;
            if (batch.scanned < PENDING_BATCH_SIZE) {
                ownPendingCursor = null;
                return;
            }
        }
    }

    /**
     * 扫一轮"自己的 PENDING"：XPENDING(按消费者过滤) 取回消息体 + 投递次数。
     *
     * 【为什么不套用 claimAbandonedPending 的 idle 过滤】自己处理失败的消息空闲时间是0，
     * 套上 30 秒门槛等于永远扫不到自己——重试路径和故障转移路径本来就该是两套判据。
     */
    private PendingBatch readOwnPendingBatch() {
        final Range<String> range = ownPendingCursor == null
                ? Range.unbounded()
                : Range.rightUnbounded(Range.Bound.exclusive(ownPendingCursor));
        return stringRedisTemplate.execute((RedisCallback<PendingBatch>) connection -> {
            PendingMessages pending = connection.xPending(
                    RedisConstants.SECKILL_ORDER_STREAM_KEY.getBytes(StandardCharsets.UTF_8),
                    Consumer.from(RedisConstants.SECKILL_ORDER_STREAM_GROUP, consumerName),
                    range, (long) PENDING_BATCH_SIZE);
            if (pending.isEmpty()) {
                return PendingBatch.empty();
            }
            List<String> ids = pending.stream()
                    .map(PendingMessage::getIdAsString)
                    .collect(Collectors.toList());
            String lastId = pending.get(pending.size() - 1).getIdAsString();
            Map<String, Long> counts = new HashMap<>();
            for (PendingMessage message : pending) {
                // 记的是"这次投递算第几次"：XCLAIM 本身会把计数+1，
                // 用 XPENDING 读到的旧值记账会少算一轮，等于把上限偷偷放宽一次
                counts.put(message.getIdAsString(), message.getTotalDeliveryCount() + 1);
            }
            return new PendingBatch(claimRecords(connection, ids, Duration.ZERO), counts,
                    lastId, pending.size());
        });
    }

    private void claimAbandonedPending() {
        // 【八股:XCLAIM故障转移】XPENDING列出"已投递未ACK"的消息及其归属消费者和空闲时长
        // 某消费者崩溃后其PENDING消息永远不会被ACK——另一个实例用XCLAIM把所有权
        // 抢过来重新处理。minIdleTime=30s是安全阀:只认领"明显被遗弃"的消息,
        // 不会抢走仍在正常处理中的消息(处理慢≠崩溃)
        int scanned = 0;
        while (scanned < MAX_PENDING_SCAN) {
            PendingBatch batch = readAbandonedBatch();
            if (batch == null || batch.scanned == 0) {
                abandonedPendingCursor = null;
                return;
            }
            processRecords(batch.records, batch.deliveryCounts);
            scanned += batch.scanned;
            abandonedPendingCursor = batch.lastId;
            if (batch.scanned < PENDING_BATCH_SIZE) {
                abandonedPendingCursor = null;
                return;
            }
        }
    }

    /**
     * 扫一轮"其他实例遗留"的PENDING消息：XPENDING列出 → 过滤空闲超30s的 → XCLAIM认领并反序列化。
     */
    private PendingBatch readAbandonedBatch() {
        final Range<String> range = abandonedPendingCursor == null
                ? Range.unbounded()
                : Range.rightUnbounded(Range.Bound.exclusive(abandonedPendingCursor));
        return stringRedisTemplate.execute((RedisCallback<PendingBatch>) connection -> {
            PendingMessages pending = connection.xPending(
                    RedisConstants.SECKILL_ORDER_STREAM_KEY.getBytes(StandardCharsets.UTF_8),
                    RedisConstants.SECKILL_ORDER_STREAM_GROUP, range,
                    (long) PENDING_BATCH_SIZE);
            if (pending.isEmpty()) {
                return PendingBatch.empty();
            }
            List<String> idleIds = pending.stream()
                    .filter(message -> message.getElapsedTimeSinceLastDelivery().compareTo(
                            IDLE_CLAIM_THRESHOLD) >= 0)
                    .map(PendingMessage::getIdAsString)
                    .collect(Collectors.toList());
            String lastId = pending.get(pending.size() - 1).getIdAsString();
            Map<String, Long> counts = new HashMap<>();
            for (PendingMessage message : pending) {
                counts.put(message.getIdAsString(), message.getTotalDeliveryCount() + 1);
            }
            if (idleIds.isEmpty()) {
                return new PendingBatch(Collections.emptyList(), Collections.emptyMap(), lastId, pending.size());
            }
            return new PendingBatch(claimRecords(connection, idleIds, IDLE_CLAIM_THRESHOLD), counts,
                    lastId, pending.size());
        });
    }

    /**
     * XCLAIM认领消息并反序列化为 MapRecord（ByteRecord → StringSerializer 解码）
     *
     * @param minIdle 只认领空闲超过该时长的消息。认领自己的消息传 {@link Duration#ZERO}
     *                （自己处理失败的消息空闲时间是0，套用30秒门槛会永远扫不到自己）
     */
    private List<MapRecord<String, Object, Object>> claimRecords(
            RedisConnection connection, List<String> ids, Duration minIdle) {
        List<ByteRecord> records = connection.xClaim(
                RedisConstants.SECKILL_ORDER_STREAM_KEY.getBytes(StandardCharsets.UTF_8),
                RedisConstants.SECKILL_ORDER_STREAM_GROUP,
                consumerName,
                RedisStreamCommands.XClaimOptions.minIdle(minIdle).ids(ids.toArray(new String[0])));
        List<MapRecord<String, Object, Object>> claimed = new ArrayList<>();
        for (ByteRecord record : records) {
            MapRecord<String, String, String> deserialized = record.deserialize(
                    stringRedisTemplate.getStringSerializer(),
                    stringRedisTemplate.getStringSerializer(),
                    stringRedisTemplate.getStringSerializer());
            @SuppressWarnings({"unchecked", "rawtypes"})
            MapRecord<String, Object, Object> converted = (MapRecord) deserialized;
            claimed.add(converted);
        }
        return claimed;
    }

    private void processRecords(List<MapRecord<String, Object, Object>> records,
                                Map<String, Long> deliveryCounts) {
        // 【八股:消费端必须幂等】Stream重投/claim/重启都会造成同一条消息被处理多次
        // 兜底在handleVoucherOrder内:Redisson锁(用户+券粒度) + 订单主键存在性检查,
        // "处理过"直接返回ALREADY_PROCESSED,重复处理是无害的
        if (records == null || records.isEmpty()) {
            return;
        }
        for (MapRecord<String, Object, Object> record : records) {
            // 【链路追踪】逐条消息接力 traceId：本线程是常驻单线程、一轮最多取10条，
            // 不逐条重置的话这一整轮日志都会挂着第一条消息的 id。
            // id 来自 seckill.lua 写进 Stream 的字段，于是"用户那次HTTP请求 → 这里落库"是同一个串。
            try (TraceContext.Scope ignored = TraceContext.enter(recordTraceId(record))) {
                Long delivered = deliveryCounts == null ? null
                        : deliveryCounts.get(record.getId().getValue());
                // 【八股:重试必须有终点】前三层保证消息不丢,但都不管"永远成功不了"的消息。
                // 到顶就走 deadLetter,不再让它继续占着库存和一人一单资格空转。
                if (delivered != null && delivered >= MAX_DELIVERY_COUNT) {
                    deadLetter(record, delivered);
                    continue;
                }
                processRecord(record, delivered);
            }
        }
    }

    /**
     * 毒丸消息的终点：移出消费循环 + 回滚预占。
     *
     * 【为什么必须有这一步】在此之前，处理失败的消息只会被无限重投：每轮循环重读一次
     * PENDING、报一次错、原样留着。一条"业务上必然失败"的消息（依赖的券已被删除、
     * 或某次数据修复后必然抛异常）就这样让 Redis 库存永久少 1、让该用户的一人一单资格
     * 被永久占用，而日志里只有刷屏——没有任何地方说得出"这张券少卖了"。
     *
     * 【为什么是回滚而不是继续重试】投递次数已经到顶，说明这不是抖动而是确定性失败。
     * 继续占着库存，代价是确定的（少卖一张、一个用户被永久挡住），收益是不确定的。
     * 回滚本身是幂等的（restore-seckill.lua 的 seckill:restored:{orderId} NX 锁），
     * 所以"消息其实已经落库成功、只是失败在后续步骤"这种情况下重复回滚也不会多恢复库存。
     */
    private void deadLetter(MapRecord<String, Object, Object> record, long delivered) {
        log.error("秒杀订单消息已投递{}次仍未成功，判定为毒丸，移出消费循环并回滚预占: id={}, value={}",
                delivered, record.getId(), record.getValue());
        countConsume("dead_letter");
        try {
            voucherOrderService.releaseRejectedReservation(toOrder(record.getValue()), true, true);
        } catch (RuntimeException e) {
            // 消息体解不开正是它成为毒丸的原因之一，此时不能凭猜测回滚。
            // 但也不能就此丢掉：全局预扣索引里还留着这条登记，
            // SeckillReservationReconciler 会从"扣了库存却没有订单"那一侧把它扫出来。
            log.error("毒丸消息回滚预占失败，交由对账任务兜底: id={}", record.getId(), e);
            countConsume("dead_letter_rollback_failed");
        }
        acknowledgeAndDelete(record);
    }

    /** 消费结果计数：这几个出口以前只有日志，出问题时全靠人翻日志 */
    private void countConsume(String result) {
        meterRegistry.counter("dianping.seckill.consumer", "result", result).increment();
    }

    private void processRecord(MapRecord<String, Object, Object> record, Long delivered) {
        try {
            VoucherOrder order = toOrder(record.getValue());
            OrderCreationResult result = voucherOrderService.handleVoucherOrder(order);
            if (result == OrderCreationResult.ACTIVE_ORDER_EXISTS) {
                // 【八股:失败补偿】重复单:回滚Redis库存,但保留一人一单资格(防止反复穿透)
                voucherOrderService.releaseRejectedReservation(order, true, false);
                countConsume("rejected_active_order");
            } else if (result == OrderCreationResult.OUT_OF_STOCK) {
                // Redis was ahead of MySQL. Keep the pre-decrement so cached stock converges to DB.
                // 【八股:不回滚的智慧】DB库存不足说明Redis领先——保留Redis预扣,
                // 让缓存库存向DB收敛,回滚反而会造成Redis超卖
                voucherOrderService.releaseRejectedReservation(order, false, true);
                countConsume("rejected_out_of_stock");
            } else {
                sendOrderDelayMessage(order);
                countConsume("created");
            }
            acknowledgeAndDelete(record);
        } catch (IllegalArgumentException e) {
            // 【已知缺口:丢弃 ≠ 已补偿】走到这里说明订单字段不完整，连 orderId 都可能取不到，
            // 所以无法在这里回滚。此时 Redis 的预扣是实打实发生过的（Lua 原子扣了库存、
            // 占了一人一单），只能交给全局预扣索引 + SeckillReservationReconciler 从对账侧收回。
            log.error("丢弃非法秒杀订单消息（预占交由对账任务回收）: id={}", record.getId(), e);
            countConsume("discarded");
            acknowledgeAndDelete(record);
        } catch (RuntimeException e) {
            // 保留 pending 等下一轮重试；到 MAX_DELIVERY_COUNT 会走 deadLetter。
            // 日志带上"第几次投递"：次数在往上涨而错误不变，就是在烧重试预算，
            // 这是区分"抖一下就好"和"这条消息永远不会成功"的唯一现场证据。
            log.error("秒杀订单消息处理失败，保留pending等待重试: id={}, 已投递{}次/上限{}",
                    record.getId(), delivered == null ? 1 : delivered, MAX_DELIVERY_COUNT, e);
            countConsume("failed");
        }
    }

    /**
     * 取消息里的 traceId。
     *
     * 【改造之前入队的消息没有这个字段】返回 null，由 TraceContext.enter 现生成一个——
     * 上游那一棒确实断了（没得接），但消费端之后的日志（含它转发的延迟消息）仍共用一个新 id。
     */
    private static String recordTraceId(MapRecord<String, Object, Object> record) {
        Object traceId = record.getValue().get("traceId");
        return traceId instanceof CharSequence ? traceId.toString() : null;
    }

    private void acknowledgeAndDelete(MapRecord<String, Object, Object> record) {
        stringRedisTemplate.opsForStream().acknowledge(
                RedisConstants.SECKILL_ORDER_STREAM_KEY,
                RedisConstants.SECKILL_ORDER_STREAM_GROUP,
                record.getId());
        try {
            stringRedisTemplate.opsForStream().delete(
                    RedisConstants.SECKILL_ORDER_STREAM_KEY, record.getId());
        } catch (RuntimeException e) {
            log.warn("删除已完成秒杀Stream记录失败: id={}", record.getId(), e);
        }
    }

    private VoucherOrder toOrder(Map<Object, Object> values) {
        VoucherOrder order = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
        if (order.getCreateTime() == null) {
            Object epoch = values.get("createEpoch");
            long epochSecond = epoch == null ? Instant.now().getEpochSecond()
                    : Long.parseLong(epoch.toString());
            order.setCreateTime(LocalDateTime.ofInstant(
                    Instant.ofEpochSecond(epochSecond), ZoneId.systemDefault()));
        }
        order.setStatus(OrderStatus.UNPAID.getCode());
        if (order.getId() == null || order.getUserId() == null || order.getVoucherId() == null) {
            throw new IllegalArgumentException("秒杀订单消息字段不完整");
        }
        return order;
    }

    private void sendOrderDelayMessage(VoucherOrder order) {
        // 【八股:消息级TTL动态扣减】异步落库有延迟,若固定发30分钟TTL,
        // 实际超时窗口会变成"落库时刻起30分钟"(比用户预期长)
        // 按下单时间算剩余:remaining = 30min - 已流逝时间,保证"下单起30分钟"精确取消
        // 注意:消息级expiration与队列级x-message-ttl并存时取较小者生效
        long elapsedMillis = order.getCreateTime() == null ? 0L
                : Math.max(0L, Duration.between(
                        order.getCreateTime(), LocalDateTime.now()).toMillis());
        long remainingMillis = Math.max(1L, QueueConfig.ORDER_DELAY_TTL - elapsedMillis);
        MessagePostProcessor expiration = message -> {
            message.getMessageProperties().setExpiration(String.valueOf(remainingMillis));
            return message;
        };
        confirmedRabbitPublisher.send(
                QueueConfig.ORDER_DELAY_EXCHANGE,
                QueueConfig.ORDER_DELAY_ROUTING_KEY,
                order.getId().toString(), expiration, "order-delay:" + order.getId());
    }

    private static final class PendingBatch {
        private final List<MapRecord<String, Object, Object>> records;
        /** recordId → 本次投递算第几次；只对 XPENDING + XCLAIM 这条路径有值 */
        private final Map<String, Long> deliveryCounts;
        private final String lastId;
        private final int scanned;

        private PendingBatch(List<MapRecord<String, Object, Object>> records,
                             Map<String, Long> deliveryCounts, String lastId, int scanned) {
            this.records = records;
            this.deliveryCounts = deliveryCounts;
            this.lastId = lastId;
            this.scanned = scanned;
        }

        private static PendingBatch empty() {
            return new PendingBatch(Collections.emptyList(), Collections.emptyMap(), null, 0);
        }
    }
}
