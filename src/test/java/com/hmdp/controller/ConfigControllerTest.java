package com.hmdp.controller;

import com.hmdp.aspect.RateLimitAspect;
import com.hmdp.dto.Result;
import com.hmdp.listener.TransactionOutboxPublisher;
import com.hmdp.utils.DynamicConfig;
import com.hmdp.utils.RedisConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * ConfigController 单元测试 —— 管理接口唯一的价值是「它报的数是真的」，所以断言都压在回显上。
 *
 * <p>
 * 这里用一个内存版 DynamicConfig 替身，而不是往 mock 上 stub 返回值：
 * 这个类做的事就是「写进去 → 下一次读出来对得上」，用 mock 自证等于什么都没测。
 *
 * <p>
 * 不测鉴权：{@code /config/**} 靠 MvcConfig 的登录拦截器兜，那是拦截器自己的用例该管的事。
 */
@DisplayName("ConfigController 运行期配置管理接口")
class ConfigControllerTest {

    private static final String API = "com.hmdp.controller.VoucherOrderController.seckillVoucher";

    private final ConfigController controller = new ConfigController();
    private final InMemoryConfig config = new InMemoryConfig();
    private final RateLimitAspect aspect = mock(RateLimitAspect.class);
    private final TransactionOutboxPublisher publisher = mock(TransactionOutboxPublisher.class);

    @BeforeEach
    void injectCollaborators() {
        ReflectionTestUtils.setField(controller, "dynamicConfig", config);
        ReflectionTestUtils.setField(controller, "rateLimitAspect", aspect);
        ReflectionTestUtils.setField(controller, "outboxPublisher", publisher);

        Map<String, Double> defaults = new LinkedHashMap<>();
        defaults.put(API, 50.0D);
        when(aspect.annotationDefaults()).thenReturn(defaults);
        when(publisher.effectiveIntervalMs()).thenReturn(1000L);
    }

    // ---------------------------------------------------------------- 总览

    @Test
    @DisplayName("总览三列齐全：注解默认值 / 覆盖值 / 实际生效值")
    void overviewShowsDefaultOverrideAndEffective() {
        config.store.put(RedisConstants.RATE_LIMIT_RULE_KEY + "com.hmdp.controller.UserController.login", "5");

        List<Map<String, Object>> entries = rateLimitEntries(data(controller.list()));

        Map<String, Object> untouched = entryOf(entries, API);
        assertEquals(50.0, untouched.get("annotationQps"));
        assertNull(untouched.get("override"), "没配过覆盖时这一列必须是 null，不能是 0 或空串");
        assertEquals(50.0, untouched.get("effectiveQps"));
        assertEquals(Boolean.FALSE, untouched.get("overridden"));

        Map<String, Object> overridden = entryOf(entries, "com.hmdp.controller.UserController.login");
        assertEquals("5", overridden.get("override"));
        assertEquals(5.0, overridden.get("effectiveQps"));
        assertEquals(Boolean.TRUE, overridden.get("overridden"));
    }

    @Test
    @DisplayName("只有覆盖键、代码里没这个接口：也列出来，并说明默认值未知")
    void ghostRuleIsVisibleNotSilentlySkipped() {
        config.store.put(RedisConstants.RATE_LIMIT_RULE_KEY + "com.hmdp.deleted.OldController.old", "9");

        Map<String, Object> ghost = entryOf(rateLimitEntries(data(controller.list())),
                "com.hmdp.deleted.OldController.old");

        assertNull(ghost.get("annotationQps"));
        assertEquals(9.0, ghost.get("effectiveQps"));
        assertTrue(ghost.get("note") != null, "登记表里没有的 api 必须自带说明，否则运维会以为漏读了");
    }

    // ---------------------------------------------------------------- 写限流规则

    @Test
    @DisplayName("写入后立刻能读回同一个生效值")
    void putIsImmediatelyVisibleInGet() {
        Result put = controller.putRateLimit(API, 8.0);
        assertTrue(put.getSuccess());
        assertEquals(8.0, singleEntry(put).get("effectiveQps"), "写完要当场回显生效值，不该让人再查一次");
        assertEquals("8.0", config.store.get(DynamicConfig.rateLimitRuleKey(API)));

        Map<String, Object> echo = singleEntry(controller.getRateLimit(API));
        assertEquals(8.0, echo.get("effectiveQps"));
        assertEquals(50.0, echo.get("annotationQps"), "覆盖不能改写默认值那一列，否则看不出被改过");
    }

    @Test
    @DisplayName("qps 越界：拒绝，并且一个键都不写")
    void rejectsOutOfRangeQps() {
        for (double bad : new double[]{0.0, 0.5, -3, DynamicConfig.MAX_QPS * 2}) {
            Result result = controller.putRateLimit(API, bad);
            assertFalse(result.getSuccess(), "qps=" + bad + " 必须被拒");
            assertTrue(config.store.isEmpty(), "被拒的写入不能留下任何键: " + bad);
        }
    }

    @Test
    @DisplayName("api 形状不对：拒绝（它是 Redis 键名的一部分）")
    void rejectsMalformedApi() {
        // 放过这些的话：花括号在 Redis Cluster 下会造出意外的 hash tag，* 会造出一个
        // 被 GET /config 的 SCAN 匹配到的幽灵键；少了点号则根本不是「类名.方法名」。
        // 都在唯一的写入口挡掉，比在读取侧兜便宜得多。
        StringBuilder tooLong = new StringBuilder("com.hmdp.X.y");
        while (tooLong.length() <= 160) {
            tooLong.append("z");
        }
        for (String bad : new String[]{"", "  ", "has space", "a{b}", "a*b", "ab", "中文.方法", tooLong.toString()}) {
            assertFalse(controller.putRateLimit(bad, 5.0).getSuccess(), "api=\"" + bad + "\" 必须被拒");
            assertTrue(config.store.isEmpty(), "被拒的写入不能留下键: " + bad);
        }
    }

    @Test
    @DisplayName("Redis 写不进去时报失败，而不是回一个 200")
    void writeFailureIsReportedNotSwallowed() {
        config.failWrites = true;

        assertFalse(controller.putRateLimit(API, 8.0).getSuccess(),
                "读侧的兜底是「按默认值继续跑」，写侧绝不能照抄 —— 那样调用方会以为改成功了");
        assertEquals(DynamicConfig.rateLimitRuleKey(API), config.lastFailedWrite);
        assertTrue(config.store.isEmpty(), "写失败不能留下半套状态");
    }

    @Test
    @DisplayName("删除覆盖 = 回到注解默认值")
    void deleteFallsBackToAnnotation() {
        controller.putRateLimit(API, 8.0);
        assertTrue(controller.deleteRateLimit(API).getSuccess());

        Map<String, Object> echo = singleEntry(controller.getRateLimit(API));
        assertNull(echo.get("override"));
        assertEquals(50.0, echo.get("effectiveQps"));
    }

    @Test
    @DisplayName("既没配过、也不在登记表里的 api：单查报「未配置」，不回显一行空值")
    void unknownApiIsNotEchoedAsBlank() {
        Result result = controller.getRateLimit("com.hmdp.nope.NotCalled.never");

        assertFalse(result.getSuccess());
        assertNull(result.getData());
    }

    @Test
    @DisplayName("生效值的口径与 Lua 逐字一致：脏值忽略、越大的值照收")
    void effectiveMirrorsTheLuaRuleNotTheWriteValidation() {
        // 写入接口挡了越界，但有人直接 redis-cli 塞值。此时 Lua 的判定是
        // "能解析成数字且 >= 1 就用"，回显必须抄同一套 —— 报一个更严的"生效值"比不报更糟。
        config.store.put(DynamicConfig.rateLimitRuleKey(API), "0.5");
        assertEquals(50.0, singleEntry(controller.getRateLimit(API)).get("effectiveQps"),
                "Lua 会忽略 <1 的覆盖值，这里也得报注解值");

        config.store.put(DynamicConfig.rateLimitRuleKey(API), "abc");
        assertEquals(50.0, singleEntry(controller.getRateLimit(API)).get("effectiveQps"));

        config.store.put(DynamicConfig.rateLimitRuleKey(API), "99999999");
        assertEquals(99999999.0, singleEntry(controller.getRateLimit(API)).get("effectiveQps"),
                "超出写入上限的值：写入接口不给配，但既然键里已经有了，就得如实报出来");
    }

    // ---------------------------------------------------------------- 写扫描间隔

    @Test
    @DisplayName("间隔的生效值由发布器给出，接口自己不再算一遍")
    void outboxEntryDelegatesTheEffectiveValue() {
        when(publisher.effectiveIntervalMs()).thenReturn(250L);

        Map<String, Object> outbox = outbox(data(controller.list()));

        assertEquals(250L, outbox.get("effectiveIntervalMs"),
                "「生效值」的口径只能由发布器定义一处；接口另算一遍就会报出一个和实际跑的不一样的数");
    }

    @Test
    @DisplayName("间隔越界：拒绝，键保持原样")
    void rejectsOutOfRangeInterval() {
        controller.putOutboxInterval(300L);
        assertFalse(controller.putOutboxInterval(50L).getSuccess(), "低于 tick 的间隔是个骗人的数字");
        assertFalse(controller.putOutboxInterval(DynamicConfig.MAX_INTERVAL_MS + 1).getSuccess());
        assertEquals("300", config.store.get(RedisConstants.OUTBOX_PUBLISH_INTERVAL_KEY));

        assertTrue(controller.deleteOutboxInterval().getSuccess());
        assertNull(config.store.get(RedisConstants.OUTBOX_PUBLISH_INTERVAL_KEY));
    }

    // ---------------------------------------------------------------- 辅助

    @SuppressWarnings("unchecked")
    private static Map<String, Object> data(Result result) {
        return (Map<String, Object>) result.getData();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> singleEntry(Result result) {
        return (Map<String, Object>) result.getData();
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> rateLimitEntries(Map<String, Object> data) {
        return (List<Map<String, Object>>) data.get("rateLimit");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> outbox(Map<String, Object> data) {
        return (Map<String, Object>) data.get("outbox");
    }

    private static Map<String, Object> entryOf(List<Map<String, Object>> entries, String api) {
        for (Map<String, Object> entry : entries) {
            if (api.equals(entry.get("api"))) {
                return entry;
            }
        }
        throw new AssertionError("列表里没有 " + api + "，实际: " + entries);
    }

    /** 行为可控的内存配置源：写→读这条闭环必须真跑一遍 */
    private static class InMemoryConfig extends DynamicConfig {
        final Map<String, String> store = new LinkedHashMap<>();
        boolean failWrites = false;
        String lastFailedWrite = null;

        @Override
        public String raw(String key) {
            return store.get(key);
        }

        @Override
        public void put(String key, String value) {
            if (failWrites) {
                lastFailedWrite = key;
                throw new RedisConnectionFailureException("connection refused");
            }
            store.put(key, value);
        }

        @Override
        public void remove(String key) {
            store.remove(key);
        }

        @Override
        public Set<String> keysWithPrefix(String prefix) {
            Set<String> hit = new LinkedHashSet<>();
            for (String key : store.keySet()) {
                if (key.startsWith(prefix)) {
                    hit.add(key);
                }
            }
            return hit;
        }
    }
}
