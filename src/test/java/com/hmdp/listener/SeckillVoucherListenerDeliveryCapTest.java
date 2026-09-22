package com.hmdp.listener;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.enums.OrderCreationResult;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.ConfirmedRabbitPublisher;
import com.hmdp.utils.RedisConstants;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 消费端的投递上限判定 —— 纯 Mockito，不需要 Redis/MySQL。
 *
 * <p>
 * 【这个测试守的是什么】投递次数到顶之后，消息必须被"回滚 + 移出循环"，而不是继续重试或
 * 直接落库。这是唯一一处消费端会主动改动库存的分支（{@link SeckillVoucherListener#deadLetter}
 * 内部，私有方法，所以经由 {@code processRecords} 这个入口来验证）。
 *
 * <p>
 * 【它覆盖不到什么，必须说清楚】投递次数表的键是 {@code recordId} 字符串，
 * 测试里是手工填的，所以<strong>"XPENDING 报出的 id 与 XCLAIM 拿回的 record id 是否真的是
 * 同一个字符串"这件事只有连真 Redis 才验得了</strong>。这里的取舍是：万一两者格式对不上，
 * 查表落空（{@code delivered == null}）会退化成"照旧重试"，也就是本次改造之前的行为——
 * 不会更糟，但上限也不会生效。
 */
@DisplayName("SeckillVoucherListener 的投递上限判定")
class SeckillVoucherListenerDeliveryCapTest {

    private static final String ENTRY_ID = "1758522123456-0";
    private static final long ORDER_ID = 1001L;
    private static final long USER_ID = 2002L;
    private static final long VOUCHER_ID = 3003L;

    private final SeckillVoucherListener listener = new SeckillVoucherListener();
    private final IVoucherOrderService voucherOrderService = mock(IVoucherOrderService.class);
    private final ConfirmedRabbitPublisher rabbitPublisher = mock(ConfirmedRabbitPublisher.class);
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final StreamOperations<String, Object, Object> streamOps = mock(StreamOperations.class);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

    @BeforeEach
    void injectCollaborators() {
        ReflectionTestUtils.setField(listener, "voucherOrderService", voucherOrderService);
        ReflectionTestUtils.setField(listener, "confirmedRabbitPublisher", rabbitPublisher);
        ReflectionTestUtils.setField(listener, "stringRedisTemplate", redis);
        ReflectionTestUtils.setField(listener, "meterRegistry", meters);
        // 不要 new 完之后调 start()/@PostConstruct：那会真起一个消费线程
        when(redis.opsForStream()).thenReturn(streamOps);
    }

    @Test
    @DisplayName("投递次数到达上限：回滚预占并移出循环，不再尝试落库")
    void deliveryCountAtCapRollsBackInsteadOfProcessing() {
        MapRecord<String, Object, Object> record = orderRecord();

        process(record, 120L);

        verify(voucherOrderService, never()).handleVoucherOrder(any());
        ArgumentCaptor<VoucherOrder> captured = ArgumentCaptor.forClass(VoucherOrder.class);
        // 两个 true 都要是 true：资格不还的话用户被永久挡住，比少卖一张更难解释
        verify(voucherOrderService).releaseRejectedReservation(captured.capture(), eq(true), eq(true));
        VoucherOrder rolledBack = captured.getValue();
        assertEquals(ORDER_ID, rolledBack.getId());
        assertEquals(USER_ID, rolledBack.getUserId());
        assertEquals(VOUCHER_ID, rolledBack.getVoucherId());
        assertEquals(1.0d, count("dead_letter"));
    }

    @Test
    @DisplayName("投递次数未到上限：照常尝试落库，不回滚")
    void deliveryCountBelowCapStillProcesses() {
        MapRecord<String, Object, Object> record = orderRecord();
        when(voucherOrderService.handleVoucherOrder(any())).thenReturn(OrderCreationResult.CREATED);

        process(record, 119L);

        verify(voucherOrderService).handleVoucherOrder(any());
        verify(voucherOrderService, never())
                .releaseRejectedReservation(any(), anyBoolean(), anyBoolean());
        assertEquals(1.0d, count("created"));
    }

    @Test
    @DisplayName("处理抛异常：留在pending等重试，既不回滚也不移出循环")
    void runtimeFailureKeepsMessagePending() {
        MapRecord<String, Object, Object> record = orderRecord();
        when(voucherOrderService.handleVoucherOrder(any()))
                .thenThrow(new IllegalStateException("订单保存失败"));

        process(record, 3L);

        verify(voucherOrderService, never())
                .releaseRejectedReservation(any(), anyBoolean(), anyBoolean());
        // 没 ACK 是关键：ACK 了就等于把这条消息丢掉，重试和毒丸判定都无从谈起
        verify(streamOps, never()).acknowledge(any(), any(), any(RecordId.class));
        assertEquals(1.0d, count("failed"));
    }

    /** 走 processRecords 这个入口（deadLetter/processRecord 都是私有的），并带上投递次数表 */
    private void process(MapRecord<String, Object, Object> record, long delivered) {
        Map<String, Long> counts = new HashMap<>();
        counts.put(ENTRY_ID, delivered);
        ReflectionTestUtils.invokeMethod(listener, "processRecords",
                Collections.singletonList(record), counts);
    }

    /** 一条字段完整的秒杀订单消息，id 固定成 ENTRY_ID 以便与投递次数表对齐 */
    private MapRecord<String, Object, Object> orderRecord() {
        Map<Object, Object> values = new HashMap<>();
        values.put("id", String.valueOf(ORDER_ID));
        values.put("userId", String.valueOf(USER_ID));
        values.put("voucherId", String.valueOf(VOUCHER_ID));
        values.put("createEpoch", String.valueOf(System.currentTimeMillis() / 1000));
        values.put("amount", "100");
        return MapRecord.create(RedisConstants.SECKILL_ORDER_STREAM_KEY, values)
                .withId(RecordId.of(ENTRY_ID));
    }

    private double count(String result) {
        Counter counter = meters.find("dianping.seckill.consumer")
                .tag("result", result)
                .counter();
        // 计数器按标签惰性创建，没走过那条分支时不存在，等价于 0 次
        return counter == null ? 0.0d : counter.count();
    }
}
