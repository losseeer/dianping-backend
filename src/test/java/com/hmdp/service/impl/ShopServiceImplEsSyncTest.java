package com.hmdp.service.impl;

import com.hmdp.entity.Shop;
import com.hmdp.listener.TransactionOutboxPublisher;
import com.hmdp.listener.TransactionOutboxWriter;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 商铺写路径的 ES 同步事件投递 —— 纯 Mockito，不起 Spring 上下文、不碰 MySQL/Redis/ES。
 *
 * <p>
 * 这条链路上最值得锁死的是<strong>「每次写入都必须投出一条新事件」</strong>。事件表上有
 * uk_event_key 唯一索引，而 {@link TransactionOutboxWriter} 撞到重复键只打一行 debug 日志，
 * 所以一旦有人把 eventKey 简化成 "es-sync:{shopId}"（看起来更像幂等键，也更顺口），
 * 后果是同一商铺只有第一次改动能同步进 ES，并且<strong>全程零报错</strong>。
 * 这种静默失效只有断言拦得住。
 */
@DisplayName("ShopServiceImpl 的 ES 同步事件投递")
class ShopServiceImplEsSyncTest {

    private static final long SHOP_ID = 1L;

    private final ShopServiceImpl service = new ShopServiceImpl();
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final TransactionOutboxWriter outboxWriter = mock(TransactionOutboxWriter.class);
    private final RedisIdWorker idWorker = mock(RedisIdWorker.class);

    @SuppressWarnings("rawtypes")
    private final ArgumentCaptor<Map> payloadCaptor = ArgumentCaptor.forClass(Map.class);

    @BeforeEach
    void injectCollaborators() {
        // baseMapper 继承自 MyBatis-Plus 的 ServiceImpl，其余是本类的 @Resource 字段；
        // 都用反射塞，避免为了可测性在业务类上开构造器
        ReflectionTestUtils.setField(service, "baseMapper", mock(ShopMapper.class));
        ReflectionTestUtils.setField(service, "stringRedisTemplate", redis);
        ReflectionTestUtils.setField(service, "outboxWriter", outboxWriter);
        ReflectionTestUtils.setField(service, "redisIdWorker", idWorker);
    }

    @Test
    @DisplayName("同一商铺连改两次：两条事件，eventKey 必须不同")
    @SuppressWarnings("unchecked")
    void everyUpdateEmitsItsOwnEvent() {
        // 每次写入取一个不同的分布式 id，这是 eventKey 唯一性的来源
        when(idWorker.nextId("outbox-es")).thenReturn(1001L, 1002L);
        stubUpdateById();

        service.update(newShop("第一次改名"));
        service.update(newShop("第二次改名"));

        ArgumentCaptor<String> keys = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Long> aggregateIds = ArgumentCaptor.forClass(Long.class);
        verify(outboxWriter, times(2)).save(keys.capture(), eq(TransactionOutboxPublisher.ES_SYNC),
                aggregateIds.capture(), anyMap());

        assertNotEquals(keys.getAllValues().get(0), keys.getAllValues().get(1),
                "eventKey 相同会被 uk_event_key 判为重复并静默丢弃 —— 第二次更新再也同步不进 ES");
        assertTrue(keys.getAllValues().get(0).startsWith("es-sync:" + SHOP_ID + ":"),
                "键里带 shopId 是为了能按商铺 grep 事件，实际: " + keys.getAllValues().get(0));
        assertEquals(Collections.nCopies(2, SHOP_ID), aggregateIds.getAllValues(),
                "aggregateId 是业务主键，两次都应是 shopId");
    }

    @Test
    @DisplayName("payload 只带 shopId，不带实体快照")
    void payloadCarriesOnlyTheId() {
        when(idWorker.nextId("outbox-es")).thenReturn(2001L);
        stubUpdateById();

        service.update(newShop("任意名字"));

        verify(outboxWriter).save(any(String.class), eq(TransactionOutboxPublisher.ES_SYNC),
                eq(SHOP_ID), payloadCaptor.capture());
        assertEquals(Collections.singletonMap("shopId", SHOP_ID), payloadCaptor.getValue(),
                "只带 id：消费端以 MySQL 为准回查，实体快照到了投递时已经过期");
    }

    @Test
    @DisplayName("更新仍然要删缓存（同步事件没有挤掉 Cache Aside）")
    void updateStillEvictsCache() {
        when(idWorker.nextId("outbox-es")).thenReturn(3001L);
        stubUpdateById();

        service.update(newShop("x"));

        verify(redis).delete(RedisConstants.CACHE_SHOP_KEY + SHOP_ID);
    }

    @Test
    @DisplayName("新增商铺同样投事件，且用数据库回填的主键")
    void saveEmitsEventWithGeneratedId() {
        ShopMapper mapper = baseMapper();
        when(idWorker.nextId("outbox-es")).thenReturn(4001L);
        // 真实行为：insert 之后 MyBatis 把自增 id 回填进实体，这里模拟回填
        when(mapper.insert(any(Shop.class))).thenAnswer(invocation -> {
            ((Shop) invocation.getArgument(0)).setId(99L);
            return 1;
        });

        assertTrue(service.save(newShop("新店")));

        verify(outboxWriter).save(eq("es-sync:99:4001"), eq(TransactionOutboxPublisher.ES_SYNC),
                eq(99L), anyMap());
    }

    private void stubUpdateById() {
        when(baseMapper().updateById(any(Shop.class))).thenReturn(1);
    }

    private ShopMapper baseMapper() {
        return (ShopMapper) ReflectionTestUtils.getField(service, "baseMapper");
    }

    private static Shop newShop(String name) {
        Shop shop = new Shop();
        shop.setId(SHOP_ID);
        shop.setName(name);
        return shop;
    }
}
