package com.hmdp.utils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * DynamicConfig 单元测试 —— 只测「取值口径」，不碰 Redis。
 *
 * <p>
 * 这个类的价值全在于它的兜底规则会不会骗人：读不到 = 默认值，越界 = 默认值，
 * 脏数据 = 默认值。三条都得钉住，因为它们对外的表现完全相同（配置没生效），
 * 只有测试能区分"真的没配"和"配错了所以静默失效"。
 */
@DisplayName("DynamicConfig 运行期配置读写")
class DynamicConfigTest {

    private static final String KEY = RedisConstants.OUTBOX_PUBLISH_INTERVAL_KEY;

    private final StringRedisTemplate template = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> values = mock(ValueOperations.class);

    private final DynamicConfig config = injected();

    private DynamicConfig injected() {
        when(template.opsForValue()).thenReturn(values);
        DynamicConfig c = new DynamicConfig();
        ReflectionTestUtils.setField(c, "stringRedisTemplate", template);
        return c;
    }

    // ---------------------------------------------------------------- raw

    @Test
    @DisplayName("键不存在返回 null，而不是 0 或空串")
    void missingKeyIsNull() {
        assertNull(config.raw(KEY));
    }

    @Test
    @DisplayName("Redis 读不到按「未配置」处理，异常不外泄")
    void redisFailureIsTreatedAsMissing() {
        // 只测超时不测连接失败：真出问题时先撞上的就是超时，
        // 而且它和"键不存在"走的是同一个兜底分支。
        when(values.get(KEY)).thenThrow(new QueryTimeoutException("timed out"));

        assertNull(config.raw(KEY));
    }

    // ---------------------------------------------------------------- getLong

    @Test
    @DisplayName("区间内的值原样取回，两端都是闭区间")
    void returnsValueInsideTheInclusiveRange() {
        given("250");
        assertEquals(250L, config.getLong(KEY, 1000L, 200L, 10_000L));

        given("200");
        assertEquals(200L, config.getLong(KEY, 1000L, 200L, 10_000L), "下界必须取得到，否则 200ms 根本配不上");

        given("10000");
        assertEquals(10_000L, config.getLong(KEY, 1000L, 200L, 10_000L));
    }

    @Test
    @DisplayName("越界的值回落到默认值，而不是夹到边界")
    void outOfRangeFallsBackInsteadOfClamping() {
        // 夹到边界 = "你配了 50，我就当你配了 200" —— 这和"按你说的办不了，回默认"
        // 是两种语义。后者才对：默认值是代码里 review 过的数，边界值不是任何人选的。
        given("50");
        assertEquals(1000L, config.getLong(KEY, 1000L, 200L, 10_000L));

        given("999999");
        assertEquals(1000L, config.getLong(KEY, 1000L, 200L, 10_000L));
    }

    @Test
    @DisplayName("非数字、空串、带小数点一律回落")
    void garbageFallsBack() {
        for (String garbage : Arrays.asList("abc", "1.5", "", "   ", "10s")) {
            given(garbage);
            assertEquals(1000L, config.getLong(KEY, 1000L, 200L, 10_000L),
                    "脏值 \"" + garbage + "\" 必须回落到默认值");
        }
    }

    @Test
    @DisplayName("值两端留空格仍能读出来（redis-cli 手输常带）")
    void toleratesSurroundingWhitespace() {
        given("  800  ");
        assertEquals(800L, config.getLong(KEY, 1000L, 200L, 10_000L));
    }

    // ---------------------------------------------------------------- 键名约定

    @Test
    @DisplayName("规则键与桶键共用同一个 api 尾缀")
    void ruleKeySharesTheApiSuffixWithTheBucket() {
        String api = "com.hmdp.controller.PaymentController.create";
        String rule = DynamicConfig.rateLimitRuleKey(api);

        assertEquals(RedisConstants.RATE_LIMIT_RULE_KEY + api, rule);
        // 拼法只此一处：rate-limit.lua 用同一个 methodKey 拼两个键，这里若多拼/少拼，
        // 脚本读到的就是一个从来没人写过的键 —— 覆盖静默失效，且没有任何报错。
        assertEquals(api, rule.substring(RedisConstants.RATE_LIMIT_RULE_KEY.length()));
    }

    // ---------------------------------------------------------------- 列举

    @Test
    @DisplayName("列举用 SCAN 且按前缀匹配，绝不用 KEYS")
    void listsWithScanNotKeys() {
        RedisConnection connection = mock(RedisConnection.class);
        stubExecute(connection);
        when(connection.scan(any(ScanOptions.class))).thenReturn(cursorOf(KEY));

        Set<String> keys = config.keysWithPrefix("config:outbox:");

        assertEquals(Collections.singletonList(KEY), new ArrayList<>(keys));
        ArgumentCaptor<ScanOptions> captor = ArgumentCaptor.forClass(ScanOptions.class);
        verify(connection).scan(captor.capture());
        assertEquals("config:outbox:*", captor.getValue().getPattern());
        assertTrue(captor.getValue().getCount() > 1,
                "count 太小会把一次列举变成上百次往返；太大又会单次卡住 Redis 主线程");
        // 这条是本用例的存在理由：KEYS 是 O(N) 不分片，一次就能把整个实例卡住
        verify(connection, never()).keys(any());
    }

    @Test
    @DisplayName("重复键只出现一次，且字节按 UTF-8 解码")
    void deduplicatesAndDecodesUtf8() {
        RedisConnection connection = mock(RedisConnection.class);
        stubExecute(connection);
        when(connection.scan(any(ScanOptions.class))).thenReturn(cursorOf(
                "config:ratelimit:rule:川A.方法", "config:ratelimit:rule:川A.方法"));

        assertEquals(Collections.singleton("config:ratelimit:rule:川A.方法"),
                config.keysWithPrefix(RedisConstants.RATE_LIMIT_RULE_KEY));
    }

    @Test
    @DisplayName("列举失败返回空集合，不影响任何判定")
    void scanFailureYieldsEmpty() {
        when(template.execute(any(RedisCallback.class))).thenThrow(new QueryTimeoutException("timed out"));

        assertTrue(config.keysWithPrefix("config:").isEmpty());
    }

    // ---------------------------------------------------------------- 写入

    @Test
    @DisplayName("put / remove 直接落到指定键，不加任何隐式 TTL")
    void writesHaveNoImplicitExpiry() {
        // 这里刻意没有 TTL：配置覆盖必须活到有人删它为止。
        // 一旦哪天手滑加了过期，"三个月后阈值悄悄回到注解值" 就成了一个查不出来的现象。
        config.put(KEY, "300");
        verify(values).set(KEY, "300");

        config.remove(KEY);
        verify(template).delete(KEY);
    }

    // ---------------------------------------------------------------- 辅助

    private void given(String value) {
        when(values.get(KEY)).thenReturn(value);
    }

    @SuppressWarnings("unchecked")
    private void stubExecute(RedisConnection connection) {
        when(template.execute(any(RedisCallback.class))).thenAnswer(invocation ->
                ((RedisCallback<Object>) invocation.getArgument(0)).doInRedis(connection));
    }

    /** 真实 Cursor 的替身：游标语义在这里唯一要紧的是"迭代完自动结束"，mock 反而难写 */
    private static Cursor<byte[]> cursorOf(String... keys) {
        List<byte[]> bytes = new ArrayList<>();
        for (String key : keys) {
            bytes.add(key.getBytes(StandardCharsets.UTF_8));
        }
        Iterator<byte[]> iterator = bytes.iterator();
        return new Cursor<byte[]>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public byte[] next() {
                return iterator.next();
            }

            @Override
            public void close() {
                // 生产实现里 close 会关掉底层连接游标；替身没有资源要释放
            }

            // 游标句柄/定位相关的几个方法，被测代码只看 hasNext/next/close，这里给常量即可
            @Override
            public boolean isClosed() {
                return false;
            }

            @Override
            public long getCursorId() {
                return 0L;
            }

            @Override
            public long getPosition() {
                return 0L;
            }

            @Override
            public Cursor<byte[]> open() {
                return this;
            }
        };
    }
}
