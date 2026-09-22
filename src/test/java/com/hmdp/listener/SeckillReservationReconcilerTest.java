package com.hmdp.listener;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisConstants;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.connection.stream.ByteRecord;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * 秒杀预占对账的判定矩阵 —— 纯 Mockito，不需要 Redis/MySQL。
 *
 * <p>
 * 【为什么这个类的测试值得写】对账是全链路唯一一处<strong>会主动改动库存</strong>的兜底逻辑：
 * 别的环节判错最多是"该做的事没做"，这里判错会凭空多还一份库存出去。所以下面按
 * 判定结果逐条把边界钉住，其中{@code inFlightMessageIsNotReverted}是最要紧的一条。
 *
 * <p>
 * 断言同样走 outcome 计数器（{@code dianping.seckill.reconcile{result}}）而不是去
 * 解析 Redis 命令，理由与 {@link TransactionOutboxPublisherEsSyncTest} 一致：
 * 计数器既是运维看这条链路健康度的入口，也正是几个分支之间唯一的语义差别。
 */
@DisplayName("SeckillReservationReconciler 的判定矩阵")
class SeckillReservationReconcilerTest {

    /** 索引 member 的格式见 seckill.lua 3.9：{orderId}:{userId}:{voucherId}:{streamEntryId} */
    private static final String MEMBER = "1001:2002:3003:1758522123456-0";
    private static final long ORDER_ID = 1001L;
    private static final long USER_ID = 2002L;
    private static final long VOUCHER_ID = 3003L;

    private final SeckillReservationReconciler reconciler = new SeckillReservationReconciler();
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final ZSetOperations<String, String> zSet = mock(ZSetOperations.class);
    private final IVoucherOrderService voucherOrderService = mock(IVoucherOrderService.class);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

    @BeforeEach
    void injectCollaborators() {
        ReflectionTestUtils.setField(reconciler, "stringRedisTemplate", redis);
        ReflectionTestUtils.setField(reconciler, "voucherOrderService", voucherOrderService);
        ReflectionTestUtils.setField(reconciler, "meterRegistry", meters);
        // @Value 字段在裸 new 出来的对象上是 0：batchSize=0 会让扫描永远取不到候选，
        // 测试全部静默变成"什么都没发生"
        ReflectionTestUtils.setField(reconciler, "horizonSeconds", 900L);
        ReflectionTestUtils.setField(reconciler, "batchSize", 100);
        when(redis.opsForZSet()).thenReturn(zSet);
    }

    @Test
    @DisplayName("DB里已有订单：只清索引条目，绝不回滚")
    void confirmedOrderOnlyRemovesIndexEntry() {
        candidate(MEMBER);
        when(voucherOrderService.getById(ORDER_ID)).thenReturn(new VoucherOrder());

        reconciler.reconcile();

        verify(voucherOrderService, never())
                .releaseRejectedReservation(any(), anyBoolean(), anyBoolean());
        verify(zSet).remove(RedisConstants.SECKILL_PENDING_INDEX_KEY, MEMBER);
        assertEquals(1.0d, count("confirmed"));
    }

    /**
     * 这条是整个对账逻辑的安全底线：把"时间久了还没落库"直接当成丢失来回滚，
     * 会把一条还在排队（消费者停摆、或正在重试）的消息的库存提前还回去，
     * 等它稍后被消费成功，同一份库存就被卖了两次。
     */
    @Test
    @DisplayName("消息仍在Stream里：判在途，既不回滚也不清索引")
    void inFlightMessageIsNotReverted() {
        candidate(MEMBER);
        when(voucherOrderService.getById(ORDER_ID)).thenReturn(null);
        streamHasEntry(true);

        reconciler.reconcile();

        verify(voucherOrderService, never())
                .releaseRejectedReservation(any(), anyBoolean(), anyBoolean());
        verify(zSet, never()).remove(any(), any());
        assertEquals(1.0d, count("in_flight"));
    }

    @Test
    @DisplayName("消息已从Stream消失且DB无订单：还库存也还一人一单资格，然后清索引")
    void lostMessageIsReverted() {
        candidate(MEMBER);
        when(voucherOrderService.getById(ORDER_ID)).thenReturn(null);
        streamHasEntry(false);

        reconciler.reconcile();

        ArgumentCaptor<VoucherOrder> captured = ArgumentCaptor.forClass(VoucherOrder.class);
        // 两个 true 都必须是 true：只还库存不还资格的话，该用户会被永久挡在门外
        // （Redis 侧的 SISMEMBER 还没清），而这比少卖一张更容易被用户投诉
        verify(voucherOrderService).releaseRejectedReservation(captured.capture(), eq(true), eq(true));
        VoucherOrder order = captured.getValue();
        assertEquals(ORDER_ID, order.getId());
        assertEquals(USER_ID, order.getUserId());
        assertEquals(VOUCHER_ID, order.getVoucherId());
        verify(zSet).remove(RedisConstants.SECKILL_PENDING_INDEX_KEY, MEMBER);
        assertEquals(1.0d, count("reverted"));
    }

    @Test
    @DisplayName("读库失败：本轮跳过且不清索引，留待下一轮")
    void databaseFailureKeepsIndexEntry() {
        candidate(MEMBER);
        when(voucherOrderService.getById(ORDER_ID)).thenThrow(new RuntimeException("db down"));

        reconciler.reconcile();

        // 关键在"不清索引"：把读库失败当成"已确认"会让这条残留永远消失，
        // 而它恰恰是需要被重试的那一条
        verify(zSet, never()).remove(any(), any());
        verify(voucherOrderService, never())
                .releaseRejectedReservation(any(), anyBoolean(), anyBoolean());
        assertEquals(1.0d, count("failed"));
    }

    @Test
    @DisplayName("索引member格式非法：清掉脏数据，不查库也不碰Stream")
    void malformedMemberIsDropped() {
        candidate("not-a-valid-member");

        reconciler.reconcile();

        verifyNoInteractions(voucherOrderService);
        verify(redis, never()).execute(any(RedisCallback.class));
        verify(zSet).remove(RedisConstants.SECKILL_PENDING_INDEX_KEY, "not-a-valid-member");
        assertEquals(1.0d, count("malformed"));
    }

    @Test
    @DisplayName("没有到期的候选条目：什么都不做")
    void emptyIndexDoesNothing() {
        when(zSet.rangeByScore(eq(RedisConstants.SECKILL_PENDING_INDEX_KEY),
                anyDouble(), anyDouble(), anyLong(), anyLong()))
                .thenReturn(Collections.emptySet());

        reconciler.reconcile();

        verifyNoInteractions(voucherOrderService);
        verify(zSet, never()).remove(any(), any());
    }

    private void candidate(String member) {
        when(zSet.rangeByScore(eq(RedisConstants.SECKILL_PENDING_INDEX_KEY),
                anyDouble(), anyDouble(), anyLong(), anyLong()))
                .thenReturn(Collections.singleton(member));
    }

    /** 模拟 XRANGE 单条查询的结果：true = 消息还在 Stream 里 */
    private void streamHasEntry(boolean present) {
        List<ByteRecord> found = present
                ? Collections.singletonList(mock(ByteRecord.class))
                : Collections.emptyList();
        when(redis.execute(any(RedisCallback.class))).thenReturn(found);
    }

    private double count(String result) {
        Counter counter = meters.find("dianping.seckill.reconcile")
                .tag("result", result)
                .counter();
        // 计数器按标签惰性创建，没走过那条分支时不存在，等价于 0 次
        return counter == null ? 0.0d : counter.count();
    }
}
