package com.hmdp.listener;

import com.hmdp.mapper.TransactionOutboxMapper;
import com.hmdp.utils.DynamicConfig;
import com.hmdp.utils.RedisConstants;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Outbox 发布器的扫描间隔门控 —— 覆盖「不重启也能改间隔」这条改造。
 *
 * <p>
 * 【为什么值得单独一个类】门控改变的是"这一轮到底干不干活"，出错方式很难看：
 * 间隔失效 = 回到 1 秒轮询，没人会抱怨；门控过头 = 事件永不投递，而日志上一条错误都没有。
 * 所以这里锁三件事：读取的口径（键名 + 区间 + 兜底值）、未到点时一次数据库都不碰、
 * 以及报活发生在门控之前（否则调大间隔会撞上 publisher 卡死告警）。
 */
@DisplayName("TransactionOutboxPublisher 的扫描间隔门控")
class TransactionOutboxPublisherScanIntervalTest {

    private final TransactionOutboxPublisher publisher = new TransactionOutboxPublisher();
    private final TransactionOutboxMapper outboxMapper = mock(TransactionOutboxMapper.class);
    private final DynamicConfig dynamicConfig = mock(DynamicConfig.class);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

    @BeforeEach
    void injectCollaborators() {
        ReflectionTestUtils.setField(publisher, "outboxMapper", outboxMapper);
        ReflectionTestUtils.setField(publisher, "dynamicConfig", dynamicConfig);
        ReflectionTestUtils.setField(publisher, "meterRegistry", meters);
        // @Scheduled 的 fixedDelay 与 @Value 字段在裸 new 的对象上都不生效，
        // 这里手动跑一次 @PostConstruct，让三个 gauge 注册进来。
        publisher.registerMeters();
        when(outboxMapper.selectList(any())).thenReturn(Collections.emptyList());
    }

    @Test
    @DisplayName("间隔从 Redis 键读，且带上正确的兜底值与合法区间")
    void readsTheIntervalWithTheDeclaredBounds() {
        ReflectionTestUtils.setField(publisher, "defaultIntervalMs", 1000L);
        stubInterval(5000L);

        publisher.publishPendingEvents();

        verify(dynamicConfig).getLong(eq(RedisConstants.OUTBOX_PUBLISH_INTERVAL_KEY),
                eq(1000L), eq(DynamicConfig.MIN_INTERVAL_MS), eq(DynamicConfig.MAX_INTERVAL_MS));
    }

    @Test
    @DisplayName("未到点的一轮：一次数据库都不碰，但心跳照旧报活")
    void gatedRoundTouchesNoDbButStillBeats() {
        stubInterval(DynamicConfig.MAX_INTERVAL_MS);
        // 装作"上一轮就在刚刚"，于是这一轮必定被门控挡掉
        ReflectionTestUtils.setField(publisher, "lastRoundAtMs", System.currentTimeMillis());

        assertEquals(-1, scanAgeSeconds(), "还没跑过任何一轮，指标应该报 -1 而不是 0");

        publisher.publishPendingEvents();

        verify(outboxMapper, never()).selectList(any());
        // 这条断言区分的是"心跳在门控之前还是之后"：放在之后，被挡掉的一轮就不报活，
        // 把间隔调到 10 秒 × 抓取间隔就会撞上 last_scan_age > 30s 的误报。
        assertEquals(0, scanAgeSeconds(), "被门控挡掉的一轮也必须报活");
    }

    @Test
    @DisplayName("到点的一轮才真的扫；改小间隔后立刻又该扫")
    void scansWhenDueAndAgainWhenTheIntervalShrinks() {
        // DynamicConfig 读不到键时返回的是调用方给的兜底值，这里模拟"配成 5 秒"
        stubInterval(5000L);
        publisher.publishPendingEvents();               // 首轮：lastRoundAtMs=0，必定到点
        publisher.publishPendingEvents();               // 紧接着第二轮：5 秒没到，挡掉
        verify(outboxMapper, times(1)).selectList(any());

        // 运维把它改成 200ms —— 不用重启，下一轮 tick 就该放行
        stubInterval(DynamicConfig.MIN_INTERVAL_MS);
        ReflectionTestUtils.setField(publisher, "lastRoundAtMs",
                System.currentTimeMillis() - DynamicConfig.MIN_INTERVAL_MS);
        publisher.publishPendingEvents();
        verify(outboxMapper, times(2)).selectList(any());
    }

    @Test
    @DisplayName("effectiveIntervalMs 只读不写：查询它不会推进节奏")
    void readingTheEffectiveIntervalDoesNotAdvanceTheClock() {
        stubInterval(3000L);
        ReflectionTestUtils.setField(publisher, "defaultIntervalMs", 1000L);
        long before = lastRoundAtMs();

        assertEquals(3000L, publisher.effectiveIntervalMs());

        assertEquals(before, lastRoundAtMs(),
                "管理接口每查一次就把门控往后推，等于「看一眼就把投递推迟了」");
    }

    // ---------------------------------------------------------------- 辅助

    private void stubInterval(long ms) {
        when(dynamicConfig.getLong(any(), anyLong(), anyLong(), anyLong())).thenReturn(ms);
    }

    private long lastRoundAtMs() {
        return (long) ReflectionTestUtils.getField(publisher, "lastRoundAtMs");
    }

    private int scanAgeSeconds() {
        Gauge gauge = meters.find("dianping.outbox.last_scan_age_seconds").gauge();
        double value = gauge.value();
        assertTrue(value == Math.floor(value), "秒级指标不该出现小数: " + value);
        return (int) value;
    }
}
