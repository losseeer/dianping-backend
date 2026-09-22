package com.hmdp.listener;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.enums.OrderCreationResult;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.ConfirmedRabbitPublisher;
import com.hmdp.utils.RedisConstants;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 秒杀可靠性的真机验证 —— 需要本地 Redis（其余依赖全 mock）。
 *
 * <p>
 * 【为什么必须有这个类】有两件事单测证明不了，只有真 Redis 能回答：
 * <ol>
 *   <li><b>投递次数的键对不对得上。</b>{@code SeckillVoucherListener} 用 XPENDING 给出的
 *       id 建投递次数表、再用 XCLAIM 拿回的 record id 去查。这两个字符串只要有一位不一致，
 *       查表就永远落空（{@code delivered == null}），结果是<strong>静默退化回"无限重试"</strong>——
 *       测试全绿、指标全零、毒丸永远不被判定。只有拿真 Redis 跑一遍才看得出。</li>
 *   <li><b>seckill.lua 记下的 entryId 是不是真的 Stream 条目 id。</b>对账靠 XRANGE 它来判断
 *       "消息还在不在"，这里对不上，对账就会把在途的预占误判成残留、凭空多还一份库存。</li>
 * </ol>
 *
 * <p>
 * 【为什么不用 @SpringBootTest】那会把应用上下文整个拉起来，其中包括
 * {@code SeckillVoucherListener} 自己的 {@code @PostConstruct}——它会起一个真消费线程，
 * 跟本测试抢同一个消费组 {@code g1}，消息被谁读走就成了竞态。这里手工建 Lettuce 连接，
 * 只依赖 Redis，跑起来也不需要 MySQL/ES/RabbitMQ。
 *
 * <pre>
 * 手工运行（去掉 @Disabled，或按下面这行显式放行）：
 * mvn test -Dtest=SeckillReliabilityRedisTest \
 *   -Djunit.jupiter.conditions.deactivate=org.junit.jupiter.engine.extension.DisabledCondition
 * </pre>
 *
 * <p>
 * 【代价：这个测试独占 {@code stream.orders}】监听器里的键名是常量、不接参数，
 * 所以它会动真实的 stream/group 键（每个用例前后都清干净）。
 * <strong>跑之前请先停掉应用实例</strong>，否则跑着的实例会把消息读走。
 */
@Disabled("requires local Redis（且建议先停掉应用实例）；手工验证用，CI 不跑")
@DisplayName("秒杀可靠性（真实 Redis）")
class SeckillReliabilityRedisTest {

    private static final String HOST = System.getProperty("redis.host", "127.0.0.1");
    private static final int PORT = Integer.parseInt(System.getProperty("redis.port", "6379"));

    private static final String STREAM = RedisConstants.SECKILL_ORDER_STREAM_KEY;
    private static final String GROUP = RedisConstants.SECKILL_ORDER_STREAM_GROUP;
    private static final String INDEX = RedisConstants.SECKILL_PENDING_INDEX_KEY;

    private static LettuceConnectionFactory connectionFactory;
    private static StringRedisTemplate redis;

    private final SeckillVoucherListener listener = new SeckillVoucherListener();
    private final SeckillReservationReconciler reconciler = new SeckillReservationReconciler();
    private final IVoucherOrderService voucherOrderService = mock(IVoucherOrderService.class);
    private final ConfirmedRabbitPublisher rabbitPublisher = mock(ConfirmedRabbitPublisher.class);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

    /** 本测试类自己造出来的键，都要清（应用真实的业务键一个都不能碰） */
    private static final String[] SCRATCH_KEYS = {
            "seckill:stock:9001", "seckill:stock:9002", "seckill:stock:9003",
            "seckill:order:9001", "seckill:order:9002", "seckill:order:9003",
            "seckill:order:pending:8001", "seckill:order:pending:8002", "seckill:order:pending:8003",
            "seckill:order:pending:user:7001", "seckill:order:pending:user:7002",
            "seckill:order:pending:user:7003", "seckill:restored:8003",
    };

    @BeforeAll
    static void connect() {
        connectionFactory = new LettuceConnectionFactory(HOST, PORT);
        connectionFactory.afterPropertiesSet();
        redis = new StringRedisTemplate(connectionFactory);
        redis.afterPropertiesSet();
    }

    @AfterAll
    static void disconnect() {
        connectionFactory.destroy();
    }

    @BeforeEach
    void wireAndClean() {
        ReflectionTestUtils.setField(listener, "stringRedisTemplate", redis);
        ReflectionTestUtils.setField(listener, "voucherOrderService", voucherOrderService);
        ReflectionTestUtils.setField(listener, "confirmedRabbitPublisher", rabbitPublisher);
        ReflectionTestUtils.setField(listener, "meterRegistry", meters);
        ReflectionTestUtils.setField(reconciler, "stringRedisTemplate", redis);
        ReflectionTestUtils.setField(reconciler, "voucherOrderService", voucherOrderService);
        ReflectionTestUtils.setField(reconciler, "meterRegistry", meters);
        ReflectionTestUtils.setField(reconciler, "horizonSeconds", 900L);
        ReflectionTestUtils.setField(reconciler, "batchSize", 100);
        cleanupKeys();
    }

    @AfterEach
    void cleanupKeys() {
        redis.delete(STREAM);
        redis.delete(INDEX);
        for (String key : SCRATCH_KEYS) {
            redis.delete(key);
        }
    }

    // ==================== seckill.lua ====================

    @Test
    @DisplayName("Lua 写入的全局索引：四段格式可解析，记下的 entryId 就是真实的 Stream 条目 id")
    void luaWritesResolvableReservationIndex() {
        redis.opsForValue().set("seckill:stock:9001", "5");

        Long code = runSeckillScript(8001L, 7001L, 9001L);

        assertEquals(0L, (long) code);
        Set<String> members = redis.opsForZSet().range(INDEX, 0, -1);
        assertNotNull(members);
        assertEquals(1, members.size(), "预扣成功就该登记一条待对账的预占");
        String member = members.iterator().next();
        String[] parts = member.split(":");
        // 对账的解析依赖正好四段（SeckillReservationReconciler.Reservation.parse）
        assertEquals(4, parts.length, "索引 member 格式必须正好四段，实际: " + member);
        assertEquals("8001", parts[0]);
        assertEquals("7001", parts[1]);
        assertEquals("9001", parts[2]);
        // 这条是对账安全性的地基：entryId 真在 Stream 里，XRANGE 才查得到，
        // 对账才会判"在途、不许回滚"。查不到 = 在途预占会被当成残留回滚掉。
        assertTrue(streamHasEntry(parts[3]), "记下的 entryId 必须真在 Stream 里: " + parts[3]);
        assertTrue(redis.hasKey("seckill:order:pending:8001"), "预订单也该原子写入");
    }

    // ==================== 消费端的投递上限 ====================

    @Test
    @DisplayName("投递次数到顶：真机上走死信——回滚预占、ACK 并删条目（同时证明次数表的键对得上）")
    void deliveryCountAtCapDeadLettersOnRealRedis() {
        String entryId = addOrderMessage(8001L, 7001L, 9001L);
        createGroup();
        readAsListener();
        // 把投递次数直接顶到上限-1（XCLAIM 的 RETRYCOUNT 可以一步设到位，不必循环119次）
        claimWithRetryCount(entryId, 119L);
        when(voucherOrderService.handleVoucherOrder(any()))
                .thenThrow(new IllegalStateException("落库持续失败"));

        ReflectionTestUtils.invokeMethod(listener, "processOwnPending");

        // 「没落库」+「回滚了」这两条一起成立，才说明次数表的键真的对上了：
        // 对不上则 delivered 为 null，会当成首次投递继续重试，于是既不会回滚、
        // 也会去调 handleVoucherOrder
        verify(voucherOrderService, never()).handleVoucherOrder(any());
        ArgumentCaptor<VoucherOrder> captured = ArgumentCaptor.forClass(VoucherOrder.class);
        verify(voucherOrderService).releaseRejectedReservation(captured.capture(), eq(true), eq(true));
        VoucherOrder rolledBack = captured.getValue();
        assertEquals(8001L, rolledBack.getId());
        assertEquals(7001L, rolledBack.getUserId());
        assertEquals(9001L, rolledBack.getVoucherId());
        // 移出循环 = 已 ACK 且条目已删，不能继续留在 PEL 里被反复重投
        assertTrue(pendingIds().isEmpty(), "死信之后不该再有 PENDING");
        assertFalse(streamHasEntry(entryId), "死信之后 Stream 条目该被删掉");
    }

    @Test
    @DisplayName("投递次数未到顶：真机上照常落库、ACK 并投递延迟取消消息")
    void deliveryBelowCapProcessesNormally() {
        String entryId = addOrderMessage(8001L, 7001L, 9001L);
        createGroup();
        readAsListener();
        when(voucherOrderService.handleVoucherOrder(any())).thenReturn(OrderCreationResult.CREATED);

        ReflectionTestUtils.invokeMethod(listener, "processOwnPending");

        verify(voucherOrderService).handleVoucherOrder(any());
        verify(voucherOrderService, never())
                .releaseRejectedReservation(any(), anyBoolean(), anyBoolean());
        // 延迟取消消息是"订单落库成功"之后才发的，这条链路在改造后必须仍然通
        verify(rabbitPublisher).send(any(), any(), any(), any(), any());
        assertTrue(pendingIds().isEmpty(), "处理成功之后不该再有 PENDING");
        assertFalse(streamHasEntry(entryId));
    }

    // ==================== 预占对账 ====================

    @Test
    @DisplayName("对账：消息已不在 Stream 且 DB 无订单 → 回滚；消息仍在 Stream → 绝不回滚")
    void reconcilerOnlyRevertsWhenMessageIsGone() {
        // 在途：entryId 真实存在于 Stream 里
        String liveEntry = addOrderMessage(8002L, 7002L, 9002L);
        redis.opsForZSet().add(INDEX, "8002:7002:9002:" + liveEntry, staleScore());
        // 确定丢了：先写入再删掉条目（模拟 Redis 丢数据 / 被手工 XDEL / 消息被丢弃）
        String lostEntry = addOrderMessage(8003L, 7003L, 9003L);
        redis.opsForZSet().add(INDEX, "8003:7003:9003:" + lostEntry, staleScore());
        redis.opsForStream().delete(STREAM, lostEntry);
        when(voucherOrderService.getById(anyLong())).thenReturn(null);

        reconciler.reconcile();

        // 只有丢的那条被回滚，且必须只回滚一次
        ArgumentCaptor<VoucherOrder> captured = ArgumentCaptor.forClass(VoucherOrder.class);
        verify(voucherOrderService, times(1))
                .releaseRejectedReservation(captured.capture(), eq(true), eq(true));
        assertEquals(8003L, captured.getValue().getId());
        // 在途那条的登记必须原样留着，下一轮还得靠它接着看
        assertNotNull(redis.opsForZSet().score(INDEX, "8002:7002:9002:" + liveEntry),
                "消息还在 Stream 里 = 消费者只是慢，登记绝不能摘");
        assertNull(redis.opsForZSet().score(INDEX, "8003:7003:9003:" + lostEntry),
                "已回滚的登记该摘掉");
    }

    @Test
    @DisplayName("对账：DB 里已有订单 → 只摘登记，不碰库存")
    void reconcilerConfirmsPersistedOrder() {
        String entryId = addOrderMessage(8001L, 7001L, 9001L);
        String member = "8001:7001:9001:" + entryId;
        redis.opsForZSet().add(INDEX, member, staleScore());
        when(voucherOrderService.getById(8001L)).thenReturn(new VoucherOrder());

        reconciler.reconcile();

        verify(voucherOrderService, never())
                .releaseRejectedReservation(any(), anyBoolean(), anyBoolean());
        assertNull(redis.opsForZSet().score(INDEX, member));
    }

    // ==================== 辅助 ====================

    /** 直接执行 seckill.lua（与 VoucherOrderServiceImpl 同参数顺序），绕开需要 MySQL 的入口 */
    private Long runSeckillScript(long orderId, long userId, long voucherId) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("seckill.lua"));
        script.setResultType(Long.class);
        String orderJson = "{\"id\":\"" + orderId + "\",\"userId\":\"" + userId
                + "\",\"voucherId\":\"" + voucherId + "\"}";
        return redis.execute(script, Collections.<String>emptyList(),
                String.valueOf(voucherId), String.valueOf(userId), String.valueOf(orderId),
                orderJson, String.valueOf(Instant.now().getEpochSecond()),
                String.valueOf(TimeUnit.MINUTES.toSeconds(RedisConstants.SECKILL_PENDING_ORDER_TTL)),
                "100", "trace-redis-test",
                String.valueOf(RedisConstants.SECKILL_PENDING_INDEX_TTL));
    }

    /** 造一条订单消息，返回 Stream 条目 id（字段与 seckill.lua 的 XADD 一致） */
    private String addOrderMessage(long orderId, long userId, long voucherId) {
        Map<Object, Object> values = new HashMap<>();
        values.put("id", String.valueOf(orderId));
        values.put("userId", String.valueOf(userId));
        values.put("voucherId", String.valueOf(voucherId));
        values.put("createEpoch", String.valueOf(Instant.now().getEpochSecond()));
        values.put("amount", "100");
        values.put("traceId", "trace-redis-test");
        RecordId id = redis.opsForStream().add(MapRecord.create(STREAM, values));
        assertNotNull(id, "XADD 应当返回条目 id");
        return id.getValue();
    }

    private void createGroup() {
        try {
            redis.execute((RedisCallback<Object>) connection -> connection.execute(
                    "XGROUP", "CREATE".getBytes(StandardCharsets.UTF_8),
                    STREAM.getBytes(StandardCharsets.UTF_8),
                    GROUP.getBytes(StandardCharsets.UTF_8),
                    "0".getBytes(StandardCharsets.UTF_8), "MKSTREAM".getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            if (e.getMessage() == null || !e.getMessage().contains("BUSYGROUP")) {
                throw e;
            }
        }
    }

    /**
     * 用监听器同款的读法投递一次，让条目进入 PEL（投递次数=1）。
     *
     * 【消费者名必须借监听器自己的】监听器扫自己的 PENDING 时用的是
     * {@code XPENDING ... <consumer>} 这个按消费者过滤的形式（Redis 6.2+）。
     * 这里要是随手编一个消费者名投递，消息就归属于那个不存在的消费者，
     * 监听器按自己的名字过滤时正确地看不到它 —— 实测踩过一次，
     * 表现是 handleVoucherOrder 一次都没被调用（"zero interactions"）。
     */
    private void readAsListener() {
        List<MapRecord<String, Object, Object>> records = redis.opsForStream().read(
                Consumer.from(GROUP, listenerConsumerName()),
                StreamReadOptions.empty().count(10),
                StreamOffset.create(STREAM, ReadOffset.lastConsumed()));
        assertEquals(1, records == null ? 0 : records.size(), "应当读到刚投递的那一条");
    }

    private void claimWithRetryCount(String entryId, long retryCount) {
        redis.execute((RedisCallback<Object>) connection -> connection.xClaim(
                STREAM.getBytes(StandardCharsets.UTF_8), GROUP, listenerConsumerName(),
                RedisStreamCommands.XClaimOptions.minIdle(Duration.ZERO)
                        .ids(entryId).retryCount(retryCount)));
    }

    private String listenerConsumerName() {
        return (String) ReflectionTestUtils.getField(listener, "consumerName");
    }

    private Set<String> pendingIds() {
        PendingMessages pending = redis.execute((RedisCallback<PendingMessages>) connection ->
                connection.xPending(STREAM.getBytes(StandardCharsets.UTF_8), GROUP,
                        Range.unbounded(), 100L));
        return pending == null ? Collections.emptySet()
                : pending.stream().map(PendingMessage::getIdAsString).collect(Collectors.toSet());
    }

    private boolean streamHasEntry(String entryId) {
        return !redis.opsForStream().range(STREAM, Range.closed(entryId, entryId)).isEmpty();
    }

    /** 对账窗口之外的时间戳：确保这两条会进入候选集 */
    private double staleScore() {
        return Instant.now().getEpochSecond() - 4000;
    }
}
