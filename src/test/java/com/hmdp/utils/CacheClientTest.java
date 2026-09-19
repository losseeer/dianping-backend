package com.hmdp.utils;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * CacheClient 值格式兼容测试。
 *
 * <p>
 * 【为什么单挑这一个场景】ShopServiceImpl.queryById 是"先逻辑过期、拿不到再走穿透"的双保险，
 * 两种策略共用同一个 key，但写进去的值格式不同：queryWithPassThrough 回填裸 JSON + 物理 TTL，
 * queryWithLogicalExpire 则按 RedisData 信封解析。第二次读到前者时，旧实现直接拿
 * expireTime 比时间 → NPE → 被熔断器降级成一次 DB 查询。客户端始终拿到 200，
 * 所以这个 bug 只在指标上留下一个 failure 计数，靠人永远查不出来。
 *
 * <p>
 * 这里不打 Spring 上下文，只 mock StringRedisTemplate：被测的就是这个类自己的解析分支。
 */
@DisplayName("CacheClient 缓存值格式")
class CacheClientTest {

    private static final String KEY = RedisConstants.CACHE_SHOP_KEY + "1";

    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> valueOps = mock(ValueOperations.class);
    @SuppressWarnings("unchecked")
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final CacheClient cacheClient = new CacheClient(redis, meters);

    /** dbFallback 一旦被调用就说明这次没走缓存；测试里用它证明"没回源" */
    private final Function<Long, Shop> mustNotTouchDb = id -> {
        throw new AssertionError("缓存已命中，不应查询数据库");
    };

    @BeforeEach
    void setUp() {
        when(redis.opsForValue()).thenReturn(valueOps);
    }

    @Test
    @DisplayName("逻辑过期分支读到裸 JSON → 认作有效缓存直接返回，不回源也不抛")
    void logicalExpireAcceptsPlainJsonWrittenByPassThrough() {
        Shop cached = new Shop();
        cached.setId(1L);
        cached.setName("海底捞");
        // 这正是 queryWithPassThrough 的 set() 写进 Redis 的内容
        when(valueOps.get(KEY)).thenReturn(JSONUtil.toJsonStr(cached));

        Shop result = cacheClient.queryWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY, 1L,
                Shop.class, mustNotTouchDb, 30L, TimeUnit.MINUTES);

        assertNotNull(result);
        assertEquals("海底捞", result.getName());
        // 不回填：这条值本来就在 TTL 内，重写只会把逻辑过期/物理过期两种格式再次混在一起
        verify(valueOps, never()).set(anyString(), anyString(), anyLong(), any(TimeUnit.class));
        assertEquals(1.0, lookupCount("hit_plain"));
    }

    @Test
    @DisplayName("缓存里没有该 key → 返回 null 交给穿透兜底，计 not_cached")
    void logicalExpireReportsMissingKey() {
        when(valueOps.get(KEY)).thenReturn(null);

        Shop result = cacheClient.queryWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY, 1L,
                Shop.class, mustNotTouchDb, 30L, TimeUnit.MINUTES);

        assertNull(result);
        assertEquals(1.0, lookupCount("not_cached"));
    }

    @Test
    @DisplayName("命中防穿透的空值 → 返回 null 且不查库，计 null_hit")
    void passThroughCountsNullPlaceholderAsHit() {
        // 空字符串就是 queryWithPassThrough 缓存下来的"这条数据不存在"
        when(valueOps.get(KEY)).thenReturn("");

        Shop result = cacheClient.queryWithPassThrough(RedisConstants.CACHE_SHOP_KEY, 1L,
                Shop.class, mustNotTouchDb, 30L, TimeUnit.MINUTES);

        assertNull(result);
        assertEquals(1.0, lookupCount("null_hit"));
    }

    private double lookupCount(String result) {
        Counter counter = meters.find("dianping.cache.lookup")
                .tag("cache", RedisConstants.CACHE_SHOP_KEY)
                .tag("result", result)
                .counter();
        return counter == null ? 0.0 : counter.count();
    }
}
