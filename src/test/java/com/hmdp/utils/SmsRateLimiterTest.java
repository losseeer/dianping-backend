package com.hmdp.utils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SmsRateLimiter 单元测试 —— 不启 Spring 上下文，用包级私有的 ScriptRunner 构造器注入替身。
 *
 * <p>
 * 这里**测不到** Lua 脚本本身（断言不了 Redis 的真实返回值），Lua 由
 * SmsRateLimiterLuaTest 那个需要本地 Redis 的集成测试覆盖。
 * 本测试负责锁住 Java 侧的契约：返回值映射、fail-open 边界、KEYS/ARGV 顺序与类型。
 */
@DisplayName("SmsRateLimiter 短信验证码限流器")
class SmsRateLimiterTest {

    /** 刻意取互不相同的值，这样 ARGV 的顺序断言才有意义（写错位置能被发现） */
    private static final int COOLDOWN_SECONDS = 90;
    private static final int DAILY_LIMIT = 7;
    private static final int DAILY_WINDOW_SECONDS = 86400;
    private static final int GLOBAL_LIMIT = 30;
    private static final int GLOBAL_WINDOW_SECONDS = 120;

    private static final String PHONE = "13800138000";

    private final AtomicReference<List<String>> capturedKeys = new AtomicReference<>();
    /** 故意存成 List&lt;Object&gt;，好在运行时校验元素真实类型而不只是编译期 */
    private final AtomicReference<List<Object>> capturedArgv = new AtomicReference<>();

    // ---------------------------------------------------------------- 返回值映射

    @Test
    @DisplayName("脚本返回 0 → 放行")
    void allowedWhenScriptReturnsZero() {
        assertEquals(SmsRateLimiter.Outcome.ALLOWED, limiterReturning(0L).tryAcquire(PHONE));
    }

    @Test
    @DisplayName("脚本返回 1 → 冷却中")
    void cooldownWhenScriptReturnsOne() {
        assertEquals(SmsRateLimiter.Outcome.COOLDOWN, limiterReturning(1L).tryAcquire(PHONE));
    }

    @Test
    @DisplayName("脚本返回 2 → 超日上限")
    void dailyExceededWhenScriptReturnsTwo() {
        assertEquals(SmsRateLimiter.Outcome.DAILY_EXCEEDED, limiterReturning(2L).tryAcquire(PHONE));
    }

    @Test
    @DisplayName("脚本返回 3 → 超全局上限")
    void globalExceededWhenScriptReturnsThree() {
        assertEquals(SmsRateLimiter.Outcome.GLOBAL_EXCEEDED, limiterReturning(3L).tryAcquire(PHONE));
    }

    // ---------------------------------------------------------------- fail-open 边界

    @Test
    @DisplayName("Redis 抛 DataAccessException → 放行（fail-open 契约）")
    void failsOpenWhenRedisIsDown() {
        SmsRateLimiter limiter = new SmsRateLimiter((keys, argv) -> {
            throw new RedisConnectionFailureException("connection refused");
        }, COOLDOWN_SECONDS, DAILY_LIMIT, DAILY_WINDOW_SECONDS, GLOBAL_LIMIT, GLOBAL_WINDOW_SECONDS);

        assertEquals(SmsRateLimiter.Outcome.REDIS_UNAVAILABLE, limiter.tryAcquire(PHONE));
    }

    @Test
    @DisplayName("脚本返回 null → 放行")
    void failsOpenWhenScriptReturnsNull() {
        assertEquals(SmsRateLimiter.Outcome.REDIS_UNAVAILABLE, limiterReturning(null).tryAcquire(PHONE));
    }

    @Test
    @DisplayName("脚本返回未知码 → 放行（脚本改了而 Java 没跟上时不应把用户挡在门外）")
    void failsOpenOnUnknownCode() {
        assertEquals(SmsRateLimiter.Outcome.REDIS_UNAVAILABLE, limiterReturning(99L).tryAcquire(PHONE));
    }

    @Test
    @DisplayName("非 DataAccessException 必须抛出，不能被当成降级吞掉")
    void doesNotSwallowProgrammingErrors() {
        // 这是本次改造最关键的一条防线。ARGV 若传了 int，StringRedisSerializer 会抛
        // ClassCastException —— 它是 RuntimeException，如果 catch 得宽就会被 fail-open
        // 静默吞掉，表现为"限流器完全不生效，但日志和响应看起来一切正常"。
        SmsRateLimiter limiter = new SmsRateLimiter((keys, argv) -> {
            throw new ClassCastException("java.lang.Integer cannot be cast to java.lang.String");
        }, COOLDOWN_SECONDS, DAILY_LIMIT, DAILY_WINDOW_SECONDS, GLOBAL_LIMIT, GLOBAL_WINDOW_SECONDS);

        assertThrows(ClassCastException.class, () -> limiter.tryAcquire(PHONE));
    }

    // ---------------------------------------------------------------- KEYS / ARGV 契约

    @Test
    @DisplayName("KEYS 的形状与顺序符合 Lua 契约（冷却 / 日 / 全局）")
    void buildsKeysInContractOrder() {
        limiterReturning(0L).tryAcquire(PHONE);

        assertEquals(Arrays.asList(
                RedisConstants.SMS_LIMIT_COOLDOWN_KEY + PHONE,
                RedisConstants.SMS_LIMIT_DAILY_KEY + PHONE,
                RedisConstants.SMS_LIMIT_GLOBAL_KEY
        ), capturedKeys.get(), "KEYS 顺序必须与 sms-rate-limit.lua 的 KEYS[1..3] 一致");
    }

    @Test
    @DisplayName("ARGV 的值、顺序与类型符合 Lua 契约，且全部是 String")
    void buildsArgvInContractOrder() {
        limiterReturning(0L).tryAcquire(PHONE);

        List<Object> argv = capturedArgv.get();
        assertEquals(Arrays.asList("90", "7", "86400", "30", "120"), argv,
                "ARGV 顺序必须与 sms-rate-limit.lua 的 ARGV[1..5] 一致");

        // StringRedisTemplate 用 StringRedisSerializer 序列化参数，传 int 会抛 ClassCastException。
        // 这里在运行时校验元素真实类型，锁住"ARGV 全是字符串"这个前提。
        for (Object arg : argv) {
            assertTrue(arg instanceof String, "ARGV 必须全部是 String，实际: " + arg.getClass().getName());
        }
    }

    // ---------------------------------------------------------------- 配置校验

    @Test
    @DisplayName("配置非正数时构造失败，而不是静默失效")
    void rejectsNonPositiveConfig() {
        // 冷却配 0：SET ... EX 0 是 Redis 语法错误 → 异常 → fail-open → 冷却闸门失效
        assertThrows(IllegalStateException.class, () -> new SmsRateLimiter((keys, argv) -> 0L,
                0, DAILY_LIMIT, DAILY_WINDOW_SECONDS, GLOBAL_LIMIT, GLOBAL_WINDOW_SECONDS));

        // 日上限配 -1：负数比较会让所有请求直接被拒（失败关闭，与其余配置语义不一致）
        assertThrows(IllegalStateException.class, () -> new SmsRateLimiter((keys, argv) -> 0L,
                COOLDOWN_SECONDS, -1, DAILY_WINDOW_SECONDS, GLOBAL_LIMIT, GLOBAL_WINDOW_SECONDS));

        // 日窗口配 0：EXPIRE key 0 在 Redis 里是【删除键】，日计数每次发送都被清空，
        // 每日上限永远不触发 —— 三种坑里最隐蔽的一个
        assertThrows(IllegalStateException.class, () -> new SmsRateLimiter((keys, argv) -> 0L,
                COOLDOWN_SECONDS, DAILY_LIMIT, 0, GLOBAL_LIMIT, GLOBAL_WINDOW_SECONDS));

        assertThrows(IllegalStateException.class, () -> new SmsRateLimiter((keys, argv) -> 0L,
                COOLDOWN_SECONDS, DAILY_LIMIT, DAILY_WINDOW_SECONDS, 0, GLOBAL_WINDOW_SECONDS));

        assertThrows(IllegalStateException.class, () -> new SmsRateLimiter((keys, argv) -> 0L,
                COOLDOWN_SECONDS, DAILY_LIMIT, DAILY_WINDOW_SECONDS, GLOBAL_LIMIT, 0));
    }

    // ---------------------------------------------------------------- 辅助

    private SmsRateLimiter limiterReturning(Long code) {
        return new SmsRateLimiter((keys, argv) -> {
            capturedKeys.set(keys);
            capturedArgv.set(new ArrayList<Object>(Arrays.asList(argv)));
            return code;
        }, COOLDOWN_SECONDS, DAILY_LIMIT, DAILY_WINDOW_SECONDS, GLOBAL_LIMIT, GLOBAL_WINDOW_SECONDS);
    }
}
