package com.hmdp.aspect;

import com.hmdp.annotation.RateLimit;
import com.hmdp.dto.Result;
import com.hmdp.utils.RedisRateLimiter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 限流切面 —— 基于 Redis 分布式令牌桶
 *
 * <p>
 * 【八股：AOP切面的执行流程】
 * 1. Spring在启动时扫描所有@Aspect注解的类，注册为切面
 * 2. 当请求到达Controller方法时，如果方法上有@RateLimit注解
 * 3. Spring的AOP代理拦截请求，在方法执行前调用@Around逻辑
 * 4. 切面向 {@link RedisRateLimiter} 申请令牌，拿到则放行原方法，
 *    拿不到则返回降级响应
 *
 * <p>
 * 【八股：为什么令牌桶的状态要放 Redis，而不是像以前那样放 JVM 内存？】
 * 之前这里是一个 {@code ConcurrentHashMap<String, RateLimiter>}，每个方法一个
 * Guava 令牌桶。单实例没问题，但它有两个致命缺陷：
 * 1. <b>阈值随实例数放大</b>——部署 3 个实例，配的 50 QPS 实际就是 150 QPS
 * 2. <b>重启即满桶</b>——新实例的桶是满的，每次发布都白送一波突发流量
 * 状态搬进 Redis 后，所有实例共用同一个桶，配置的阈值才是真的阈值。
 *
 * <p>
 * 【八股：为什么判定和扣减必须在同一个 Lua 脚本里？】
 * "读桶 → 算补充 → 判断够不够 → 扣减 → 写回" 是五步。分五次调用 Redis，
 * 两个并发请求就可能从同一个起点各自扣减、双双放行——限流器在并发下直接失效。
 * Lua 在 Redis 里串行执行，把这个 check-then-act 序列焊成一个原子整体。
 * 详见 {@code rate-limit.lua}。
 *
 * <p>
 * 【八股：Redis 挂了怎么办？】
 * 这里不写死，而是交给注解上的 {@code failOpen} 逐接口决定——判据是
 * "降级的代价是丢钱还是丢防护"。秒杀/支付失败关闭，登录/搜索放行。
 * 详见 {@link RateLimit#failOpen()}。
 */
@Slf4j
@Aspect
@Component
public class RateLimitAspect {

    @Resource
    private RedisRateLimiter redisRateLimiter;
    @Resource
    private MeterRegistry meterRegistry;

    @Around("@annotation(rateLimit)")
    public Object around(ProceedingJoinPoint joinPoint, RateLimit rateLimit) throws Throwable {
        String methodName = AspectFallbackSupport.getMethodName(joinPoint);

        RedisRateLimiter.Outcome outcome;
        try {
            outcome = redisRateLimiter.tryAcquire(methodName, rateLimit.qps());
        } catch (IllegalStateException e) {
            // qps 配成了 < 1（桶容量不足一个令牌 = 静默打死接口）。
            // 这是配置错误，让它带着清晰的信息炸出来，而不是变成一个查不出的死接口。
            log.error("限流配置错误，接口 {} 将不可用: {}", methodName, e.getMessage());
            throw e;
        }

        // 指标维度与下面的日志分支一一对应，不新增判定：outcome 标签刻意把
        // "Redis 不可用" 拆成 *_unavailable_allowed / *_unavailable_rejected 两个值
        // —— 只看拒绝数的话，限流真正生效和 Redis 挂了导致失败关闭会混成同一条曲线。
        switch (outcome) {
            case ALLOWED:
                count(methodName, "allowed");
                return joinPoint.proceed();

            case REDIS_UNAVAILABLE:
                // 未能判定。策略由注解决定，两条路径的日志要区分开打——
                // failOpen=false 时这里的响应体与"被限流"完全一样，
                // 不区分的话，线上根本分不清是限流生效还是 Redis 挂了。
                if (rateLimit.failOpen()) {
                    count(methodName, "unavailable_allowed");
                    log.warn("限流器不可用，按 failOpen 放行: {}", methodName);
                    return joinPoint.proceed();
                }
                count(methodName, "unavailable_rejected");
                log.error("限流器不可用，该接口配置为失败关闭，已拒绝: {}", methodName);
                return limited(joinPoint, rateLimit);

            case REJECTED:
            default:
                count(methodName, "rejected");
                log.warn("接口被限流: {} | 当前QPS限制: {}", methodName, rateLimit.qps());
                return limited(joinPoint, rateLimit);
        }
    }

    /**
     * 计数。api 标签直接用限流 key（全限定类名.方法名），与 Redis 里的桶名同源，
     * 基数等于挂了 @RateLimit 的方法数（当前 4 个），可控。
     */
    private void count(String api, String outcome) {
        meterRegistry.counter("dianping.rate.limit", "api", api, "outcome", outcome).increment();
    }

    /**
     * 被限流时的降级响应：优先调用注解指定的降级方法，否则返回统一提示。
     * 与改造前保持逐字一致，客户端无感。
     */
    private Object limited(ProceedingJoinPoint joinPoint, RateLimit rateLimit) throws Throwable {
        if (!rateLimit.fallback().isEmpty()) {
            return AspectFallbackSupport.invokeFallback(
                    joinPoint, rateLimit.fallback(), Result.fail("系统繁忙，请稍后再试"));
        }
        return Result.fail(rateLimit.message());
    }
}
