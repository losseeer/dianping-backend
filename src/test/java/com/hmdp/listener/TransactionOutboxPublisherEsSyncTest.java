package com.hmdp.listener;

import com.hmdp.entity.TransactionOutbox;
import com.hmdp.mapper.TransactionOutboxMapper;
import com.hmdp.service.IShopSearchService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.ConfirmedRabbitPublisher;
import com.hmdp.utils.DynamicConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Outbox 发布器的 ES_SYNC 分支 —— 纯 Mockito，不需要 MySQL/ES。
 *
 * <p>
 * 断言走 outcome 计数器（{@code dianping.outbox.event{type,result}}）而不是去解析
 * MyBatis-Plus 拼出来的 UPDATE SQL：计数器既是我们看这条链路健康度的入口（Grafana 的
 * Outbox 行按 type 分组），也正是 markSent / markFailed 之间唯一的语义差别，
 * 比断言 SQL 片段稳得多。
 */
@DisplayName("TransactionOutboxPublisher 的 ES_SYNC 分支")
class TransactionOutboxPublisherEsSyncTest {

    private final TransactionOutboxPublisher publisher = new TransactionOutboxPublisher();
    private final TransactionOutboxMapper outboxMapper = mock(TransactionOutboxMapper.class);
    private final IShopSearchService shopSearchService = mock(IShopSearchService.class);
    private final DynamicConfig dynamicConfig = mock(DynamicConfig.class);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

    @BeforeEach
    void injectCollaborators() {
        ReflectionTestUtils.setField(publisher, "outboxMapper", outboxMapper);
        ReflectionTestUtils.setField(publisher, "confirmedRabbitPublisher", mock(ConfirmedRabbitPublisher.class));
        ReflectionTestUtils.setField(publisher, "voucherOrderService", mock(IVoucherOrderService.class));
        ReflectionTestUtils.setField(publisher, "shopSearchService", shopSearchService);
        ReflectionTestUtils.setField(publisher, "dynamicConfig", dynamicConfig);
        ReflectionTestUtils.setField(publisher, "meterRegistry", meters);
        // @Value 字段在裸 new 出来的对象上是 0，会让第一条失败事件立刻变死信，掩盖退避逻辑
        ReflectionTestUtils.setField(publisher, "maxRetry", 20);
        // 间隔 0 = 每一轮都真扫。不喂这个的话门控会把整轮跳过，本类的用例全部空跑。
        // （Mockito 对 long 默认就返回 0，写出来是为了让这个前提显式。）
        when(dynamicConfig.getLong(any(), anyLong(), anyLong(), anyLong())).thenReturn(0L);

        // 每轮扫描开头有「回收卡死事件」和「CAS 抢占」两次 update，都返回 1 表示这一轮归本实例投递
        when(outboxMapper.update(any(), any())).thenReturn(1);
    }

    @Test
    void routesToTheHandlerWithTheIdFromThePayload() {
        stubScan(esSyncEvent("{\"shopId\":42}"));

        publisher.publishPendingEvents();

        verify(shopSearchService).syncShopById(42L);
        assertEquals(1.0d, count("sent"), "写进 ES 之后必须把事件标成已发送");
    }

    @Test
    @DisplayName("ES 写失败：退避重试，且异常绝不冒出扫描线程")
    void esFailureBecomesARetryNotACrash() {
        stubScan(esSyncEvent("{\"shopId\":42}"));
        doThrow(new RuntimeException("connect timed out"))
                .when(shopSearchService).syncShopById(42L);

        // 这一行不抛异常本身就是断言：扫描线程一旦逃出异常，整条 outbox（含支付通知）就不再投递了
        publisher.publishPendingEvents();

        assertEquals(1.0d, count("failed"), "失败要计入 failed，否则退避与死信无从观测");
        assertEquals(0.0d, count("sent"));
    }

    @Test
    @DisplayName("payload 缺 shopId：判定失败，不猜、也不碰索引")
    void malformedPayloadFailsInsteadOfGuessing() {
        stubScan(esSyncEvent("{\"shop\":42}"));

        publisher.publishPendingEvents();

        verifyNoInteractions(shopSearchService);
        assertEquals(1.0d, count("failed"));
    }

    @Test
    @DisplayName("非 ES_SYNC 事件不会被误路由")
    void otherTypesSkipTheEsBranch() {
        TransactionOutbox event = esSyncEvent("{\"shopId\":42}");
        event.setEventType(TransactionOutboxPublisher.PAY_NOTIFY);
        stubScan(event);

        publisher.publishPendingEvents();

        verify(shopSearchService, never()).syncShopById(any(Long.class));
    }

    // ---------------------------------------------------------------- 辅助

    private void stubScan(TransactionOutbox event) {
        when(outboxMapper.selectList(any())).thenReturn(Collections.singletonList(event));
    }

    private static TransactionOutbox esSyncEvent(String payload) {
        TransactionOutbox event = new TransactionOutbox();
        event.setId(7L);
        event.setEventKey("es-sync:42:1001");
        event.setEventType(TransactionOutboxPublisher.ES_SYNC);
        event.setAggregateId(42L);
        event.setPayload(payload);
        event.setStatus(0);
        event.setRetryCount(0);
        event.setNextRetryTime(LocalDateTime.now());
        event.setCreateTime(LocalDateTime.now());
        return event;
    }

    private double count(String result) {
        Counter counter = meters.find("dianping.outbox.event")
                .tag("type", TransactionOutboxPublisher.ES_SYNC)
                .tag("result", result)
                .counter();
        // 计数器的注册方式是按标签惰性创建，没走过那条分支时就是不存在，等价于 0 次
        return counter == null ? 0.0d : counter.count();
    }
}
