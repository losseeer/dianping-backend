package com.hmdp.utils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RedisRateLimiter 单元测试 —— 不启 Spring 上下文，用包级私有的 ScriptRunner 构造器注入替身。
 *
 * <p>
 * 这里**测不到** rate-limit.lua 本身（令牌怎么补、上限怎么夹），Lua 由
 * RedisRateLimiterLuaTest 那个需要本地 Redis 的集成测试覆盖。
 * 本测试锁的是 Java 侧契约：返回值映射、fail-open 边界、KEYS/ARGV 形状、本地熔断。
 */
@DisplayName("RedisRateLimiter 分布式令牌桶限流器")
class RedisRateLimiterTest {

    private static final String METHOD_KEY = "com.hmdp.controller.VoucherOrderController.seckillVoucher";
    private static final double QPS = 50.0;

    private final AtomicReference<List<String>> capturedKeys = new AtomicReference<>();
    /** 故意存成 List&lt;Object&gt;，好在运行时校验元素真实类型而不只是编译期 */
    private final AtomicReference<List<Object>> capturedArgv = new AtomicReference<>();

    // ---------------------------------------------------------------- 返回值映射

    @Test
    @DisplayName("脚本返回 1 → 放行")
    void allowedWhenScriptReturnsOne() {
        assertEquals(RedisRateLimiter.Outcome.ALLOWED, limiterReturning(1L).tryAcquire(METHOD_KEY, QPS));
    }

    @Test
    @DisplayName("脚本返回 0 → 被限流")
    void rejectedWhenScriptReturnsZero() {
        assertEquals(RedisRateLimiter.Outcome.REJECTED, limiterReturning(0L).tryAcquire(METHOD_KEY, QPS));
    }

    @Test
    @DisplayName("脚本返回 null → 不可用")
    void unavailableWhenScriptReturnsNull() {
        assertEquals(RedisRateLimiter.Outcome.REDIS_UNAVAILABLE, limiterReturning(null).tryAcquire(METHOD_KEY, QPS));
    }

    @Test
    @DisplayName("脚本返回未知码 → 不可用")
    void unavailableOnUnknownCode() {
        assertEquals(RedisRateLimiter.Outcome.REDIS_UNAVAILABLE, limiterReturning(99L).tryAcquire(METHOD_KEY, QPS));
    }

    // ---------------------------------------------------------------- 故障边界

    @Test
    @DisplayName("连接被拒 → 不可用（fail-open 契约）")
    void unavailableWhenConnectionRefused() {
        assertEquals(RedisRateLimiter.Outcome.REDIS_UNAVAILABLE,
                limiterThrowing(new RedisConnectionFailureException("connection refused")).tryAcquire(METHOD_KEY, QPS));
    }

    @Test
    @DisplayName("命令超时 → 不可用（超时才是故障时真实抛出的类型）")
    void unavailableOnCommandTimeout() {
        // Lettuce 把 RedisCommandTimeoutException 翻译成 QueryTimeoutException。
        // 多数人只测连接失败就以为覆盖了故障路径，实际线上更常见的是超时。
        assertEquals(RedisRateLimiter.Outcome.REDIS_UNAVAILABLE,
                limiterThrowing(new QueryTimeoutException("command timed out")).tryAcquire(METHOD_KEY, QPS));
    }

    @Test
    @DisplayName("非 DataAccessException 必须抛出，不能被当成降级吞掉")
    void doesNotSwallowProgrammingErrors() {
        // ARGV 若传了 int，StringRedisSerializer 会抛 ClassCastException —— 它是
        // RuntimeException，如果 catch 得宽就会被 fail-open 静默吞掉，表现为
        // "限流器完全不生效，但日志和响应看起来一切正常"。
        RedisRateLimiter limiter = new RedisRateLimiter((keys, argv) -> {
            throw new ClassCastException("java.lang.Integer cannot be cast to java.lang.String");
        });
        assertThrows(ClassCastException.class, () -> limiter.tryAcquire(METHOD_KEY, QPS));
    }

    // ---------------------------------------------------------------- 本地熔断

    @Test
    @DisplayName("连续失败 3 次后本地熔断，不再访问 Redis")
    void tripsLocalBreakerAfterConsecutiveFailures() {
        AtomicInteger calls = new AtomicInteger();
        RedisRateLimiter limiter = new RedisRateLimiter((keys, argv) -> {
            calls.incrementAndGet();
            throw new RedisConnectionFailureException("down");
        });

        for (int i = 0; i < 3; i++) {
            assertEquals(RedisRateLimiter.Outcome.REDIS_UNAVAILABLE, limiter.tryAcquire(METHOD_KEY, QPS));
        }
        assertEquals(3, calls.get(), "熔断阈值内每次都应真实尝试");

        assertEquals(RedisRateLimiter.Outcome.REDIS_UNAVAILABLE, limiter.tryAcquire(METHOD_KEY, QPS));
        assertEquals(3, calls.get(), "熔断打开后不应再访问 Redis —— 否则每个请求都要付一次超时代价");
    }

    @Test
    @DisplayName("成功一次即复位熔断计数")
    void resetsBreakerOnSuccess() {
        AtomicInteger calls = new AtomicInteger();
        RedisRateLimiter limiter = new RedisRateLimiter((keys, argv) -> {
            int n = calls.incrementAndGet();
            if (n == 3) {
                return 1L;      // 第 3 次成功，应把失败计数复位
            }
            throw new RedisConnectionFailureException("flaky");
        });

        // 1、2 失败 → 计数 2；3 成功 → 复位；4、5 再失败 → 计数回到 2
        assertEquals(RedisRateLimiter.Outcome.REDIS_UNAVAILABLE, limiter.tryAcquire(METHOD_KEY, QPS));
        assertEquals(RedisRateLimiter.Outcome.REDIS_UNAVAILABLE, limiter.tryAcquire(METHOD_KEY, QPS));
        assertEquals(RedisRateLimiter.Outcome.ALLOWED, limiter.tryAcquire(METHOD_KEY, QPS));
        assertEquals(RedisRateLimiter.Outcome.REDIS_UNAVAILABLE, limiter.tryAcquire(METHOD_KEY, QPS));
        assertEquals(RedisRateLimiter.Outcome.REDIS_UNAVAILABLE, limiter.tryAcquire(METHOD_KEY, QPS));

        // 第 6 次仍应真实调用。若计数没被复位，第 4 次就会累积到阈值(3)打开熔断，
        // 第 5、6 次会跳过 runner —— calls 会停在 4 而不是 6。
        assertEquals(RedisRateLimiter.Outcome.REDIS_UNAVAILABLE, limiter.tryAcquire(METHOD_KEY, QPS));
        assertEquals(6, calls.get(), "成功一次必须复位计数，否则零星的失败会累积成误熔断");
    }

    // ---------------------------------------------------------------- 配置校验

    @Test
    @DisplayName("qps < 1 抛异常，而不是变成静默打死接口")
    void rejectsQpsBelowOne() {
        RedisRateLimiter limiter = limiterReturning(1L);
        assertThrows(IllegalStateException.class, () -> limiter.tryAcquire(METHOD_KEY, 0.0));
        assertThrows(IllegalStateException.class, () -> limiter.tryAcquire(METHOD_KEY, -1.0));
        assertThrows(IllegalStateException.class, () -> limiter.tryAcquire(METHOD_KEY, 0.5));
    }

    // ---------------------------------------------------------------- KEYS / ARGV 契约

    @Test
    @DisplayName("KEYS 恰为两个：桶 + 规则键，都在各自的命名空间下 + 全限定方法名")
    void buildsBucketAndRuleKeys() {
        limiterReturning(1L).tryAcquire(METHOD_KEY, QPS);

        assertEquals(Arrays.asList(
                RedisConstants.RATE_LIMIT_API_KEY + METHOD_KEY,
                RedisConstants.RATE_LIMIT_RULE_KEY + METHOD_KEY), capturedKeys.get(),
                "顺序不能反：脚本里 KEYS[1] 是桶、KEYS[2] 是规则，换过来就是「拿规则键当桶写」");
    }

    @Test
    @DisplayName("ARGV 为容量与速率两个值，且运行时类型都是 String")
    void buildsArgvAsTwoStrings() {
        limiterReturning(1L).tryAcquire(METHOD_KEY, QPS);

        List<Object> argv = capturedArgv.get();
        // capacity = qps × 1.0（对齐 Guava 的 maxBurstSeconds），rate = qps
        assertEquals(Arrays.asList("50.0", "50.0"), argv,
                "容量与速率都等于 qps，说明桶容量 = qps × 1 秒；改动突发系数必须同步改这条断言");

        for (Object arg : argv) {
            assertTrue(arg instanceof String, "ARGV 必须全是 String，实际: " + arg.getClass().getName());
        }
    }

    // ---------------------------------------------------------------- 辅助

    private RedisRateLimiter limiterReturning(Long code) {
        return new RedisRateLimiter((keys, argv) -> {
            capturedKeys.set(keys);
            capturedArgv.set(new ArrayList<Object>(Arrays.asList(argv)));
            return code;
        });
    }

    private RedisRateLimiter limiterThrowing(RuntimeException e) {
        return new RedisRateLimiter((keys, argv) -> {
            throw e;
        });
    }
}
