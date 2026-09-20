package com.hmdp.aspect;

import com.hmdp.annotation.RateLimit;
import com.hmdp.dto.Result;
import com.hmdp.utils.RedisRateLimiter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Method;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * RateLimitAspect 单元测试 —— 切面在本项目里此前没有任何测试覆盖。
 *
 * <p>
 * 这里直接调用 {@code around(joinPoint, annotation)}，不需要 AspectJ 织入：
 * 注解是方法参数，JoinPoint 用 Mockito 造。重点验证「什么情况下会/不会
 * 执行原方法」——这是 4 个线上接口的实际行为契约。
 *
 * <p>
 * 每个分支的 outcome 计数也在这里断言：四个 outcome 对客户端可能长得一样
 * （rejected 与 unavailable_rejected 都是失败响应），只有指标能把它们分开，
 * 所以这份断言就是那条区分逻辑的回归网。
 */
@DisplayName("RateLimitAspect 限流切面")
class RateLimitAspectTest {

    private final RateLimitAspect aspect = new RateLimitAspect();
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

    /** 被测注解脱胎于这些方法；Mockito 没法方便地合成注解实例，用反射取真的 */
    @SuppressWarnings("unused")
    static class Fixture {
        @RateLimit(qps = 10, message = "太频繁了")
        public String plain() {
            return "ok";
        }

        @RateLimit(qps = 10, failOpen = false, message = "失败关闭了")
        public String failClosed() {
            return "ok";
        }

        @RateLimit(qps = 10, fallback = "fallbackFor")
        public String withFallback() {
            return "ok";
        }

        public String fallbackFor() {
            return "降级结果";
        }
    }

    // ---------------------------------------------------------------- 放行

    @Test
    @DisplayName("拿到令牌 → 执行原方法并原样返回")
    void proceedsWhenAllowed() throws Throwable {
        ProceedingJoinPoint jp = joinPointFor("plain", "proceeded");
        installLimiter(RedisRateLimiter.Outcome.ALLOWED);

        assertEquals("proceeded", aspect.around(jp, annotationOf("plain")));
        verify(jp, times(1)).proceed();
        assertEquals(1.0, countOf("plain", "allowed"));
    }

    // ---------------------------------------------------------------- 被限流

    @Test
    @DisplayName("被限流 → 不执行原方法，返回注解上的提示语")
    void rejectsWithoutProceeding() throws Throwable {
        ProceedingJoinPoint jp = joinPointFor("plain", "proceeded");
        installLimiter(RedisRateLimiter.Outcome.REJECTED);

        Result result = (Result) aspect.around(jp, annotationOf("plain"));

        assertFalse(result.getSuccess());
        assertEquals("太频繁了", result.getErrorMsg());
        verify(jp, never()).proceed();
        assertEquals(1.0, countOf("plain", "rejected"));
    }

    @Test
    @DisplayName("被限流且配了 fallback → 调用降级方法")
    void invokesNamedFallbackWhenLimitted() throws Throwable {
        ProceedingJoinPoint jp = joinPointFor("withFallback", "proceeded");
        installLimiter(RedisRateLimiter.Outcome.REJECTED);

        assertEquals("降级结果", aspect.around(jp, annotationOf("withFallback")));
        verify(jp, never()).proceed();
        assertEquals(1.0, countOf("withFallback", "rejected"));
    }

    // ---------------------------------------------------------------- Redis 不可用

    @Test
    @DisplayName("Redis 不可用 + failOpen=true（默认）→ 放行")
    void proceedsWhenUnavailableAndFailOpen() throws Throwable {
        ProceedingJoinPoint jp = joinPointFor("plain", "proceeded");
        installLimiter(RedisRateLimiter.Outcome.REDIS_UNAVAILABLE);

        assertEquals("proceeded", aspect.around(jp, annotationOf("plain")));
        verify(jp, times(1)).proceed();
        // 放行了，但绝不能记成 allowed —— 那是"限流器健康"的意思
        assertEquals(1.0, countOf("plain", "unavailable_allowed"));
        assertEquals(0.0, countOf("plain", "allowed"));
    }

    @Test
    @DisplayName("Redis 不可用 + failOpen=false → 拒绝（秒杀/支付走这条）")
    void rejectsWhenUnavailableAndFailClosed() throws Throwable {
        ProceedingJoinPoint jp = joinPointFor("failClosed", "proceeded");
        installLimiter(RedisRateLimiter.Outcome.REDIS_UNAVAILABLE);

        Result result = (Result) aspect.around(jp, annotationOf("failClosed"));

        assertFalse(result.getSuccess());
        assertEquals("失败关闭了", result.getErrorMsg());
        verify(jp, never()).proceed();
        // 响应体与"被限流"完全一样，两者的区别只存在于这个标签里
        assertEquals(1.0, countOf("failClosed", "unavailable_rejected"));
        assertEquals(0.0, countOf("failClosed", "rejected"));
    }

    // ---------------------------------------------------------------- 配置错误

    @Test
    @DisplayName("qps 配置非法 → 异常向上抛，不吞成降级响应")
    void propagatesConfigurationError() throws Throwable {
        ProceedingJoinPoint jp = joinPointFor("plain", "proceeded");
        RedisRateLimiter broken = mock(RedisRateLimiter.class);
        when(broken.tryAcquire(org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.anyDouble()))
                .thenThrow(new IllegalStateException("qps 必须 >= 1"));
        ReflectionTestUtils.setField(aspect, "redisRateLimiter", broken);

        try {
            aspect.around(jp, annotationOf("plain"));
            throw new AssertionError("配置错误必须抛出，不能变成查不出的死接口");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("qps"));
        }
        verify(jp, never()).proceed();
    }

    // ---------------------------------------------------------------- 注解默认值登记表

    @Test
    @DisplayName("被调用过的接口会登记注解 qps，供 GET /config 列出处置清单")
    void recordsAnnotationDefaultForCalledApis() throws Throwable {
        ProceedingJoinPoint jp = joinPointFor("plain", "proceeded");
        installLimiter(RedisRateLimiter.Outcome.ALLOWED);

        assertTrue(aspect.annotationDefaults().isEmpty(), "还没调用过，登记表应该是空的");
        aspect.around(jp, annotationOf("plain"));

        assertEquals(10.0, aspect.annotationDefaults().get(Fixture.class.getName() + ".plain"),
                "登记的是注解上的 qps，不是实际生效值 —— 覆盖值在 Redis 里，切面看不到");

        Map<String, Double> view = aspect.annotationDefaults();
        assertThrows(UnsupportedOperationException.class, () -> view.put("x", 1.0),
                "给管理接口的必须是只读视图，否则谁都能往里塞一条伪造的\"默认值\"");
    }

    // ---------------------------------------------------------------- 辅助

    @BeforeEach
    void injectMeterRegistry() {
        ReflectionTestUtils.setField(aspect, "meterRegistry", meters);
    }

    private void installLimiter(RedisRateLimiter.Outcome outcome) {
        RedisRateLimiter limiter = mock(RedisRateLimiter.class);
        when(limiter.tryAcquire(org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.anyDouble())).thenReturn(outcome);
        ReflectionTestUtils.setField(aspect, "redisRateLimiter", limiter);
    }

    /** 某个方法在某条 outcome 分支上被计了几次；该分支没走过时为 0 */
    private double countOf(String methodName, String outcome) {
        Counter counter = meters.find("dianping.rate.limit")
                .tag("api", Fixture.class.getName() + "." + methodName)
                .tag("outcome", outcome)
                .counter();
        return counter == null ? 0.0 : counter.count();
    }

    private RateLimit annotationOf(String methodName) throws NoSuchMethodException {
        return Fixture.class.getDeclaredMethod(methodName).getAnnotation(RateLimit.class);
    }

    private ProceedingJoinPoint joinPointFor(String methodName, String proceedResult) throws Throwable {
        Method method = Fixture.class.getDeclaredMethod(methodName);
        MethodSignature signature = mock(MethodSignature.class);
        when(signature.getMethod()).thenReturn(method);
        when(signature.getDeclaringType()).thenReturn(Fixture.class);
        when(signature.getParameterTypes()).thenReturn(new Class<?>[0]);

        ProceedingJoinPoint jp = mock(ProceedingJoinPoint.class);
        when(jp.getSignature()).thenReturn(signature);
        when(jp.getTarget()).thenReturn(new Fixture());
        when(jp.getArgs()).thenReturn(new Object[0]);
        when(jp.proceed()).thenReturn(proceedResult);
        return jp;
    }
}
