package com.hmdp.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 接口限流器 —— Redis + Lua 的分布式令牌桶
 *
 * <p>
 * 【八股：为什么不能再用 Guava RateLimiter？】
 * Guava 的 RateLimiter 是 JVM 进程内对象。部署 N 个实例就是 N 个独立的桶，
 * 配置的 qps 实际生效值是 N × qps —— 阈值成了摆设。更隐蔽的是实例重启：
 * 新实例的桶是满的，等于每次发布都白送一波突发流量。状态放 Redis 后，
 * 所有实例共用同一个桶，阈值才是真的阈值。
 *
 * <p>
 * 【八股：为什么用令牌桶而不是计数器？】
 * 令牌桶允许一定程度的突发（桶里攒的令牌可以一次消耗完），适合"平时低流量、
 * 偶尔高并发"的场景；计数器（固定窗口）实现更简单，但窗口边界处可能放过
 * 两倍流量。本实现与 Guava 的 SmoothBursty 语义对齐：桶容量 = qps × 1 秒，
 * 冷启动时桶是满的。更详细的算法对比见 {@code annotation/RateLimit.java}。
 *
 * <p>
 * 【失败策略不在这个类里】
 * 这里只负责判定，Redis 不可用时返回 {@link Outcome#REDIS_UNAVAILABLE}，
 * 由调用方（{@code RateLimitAspect}）根据注解上的 failOpen 决定放行还是拒绝。
 * 判断依据是「降级的代价是丢钱还是丢防护」——秒杀/支付丢钱，失败关闭；
 * 登录/搜索丢防护，放行。
 *
 * <p>
 * 【关于与 SmsRateLimiter 的重复】
 * 本类与 {@link SmsRateLimiter} 在管道代码上有约 25 行重复（ScriptRunner 测试缝、
 * 窄捕获、告警节流）。这是<strong>刻意保留</strong>的：项目自身的先例就是
 * "重复到第三次再收敛"（见 {@code AspectFallbackSupport} 的类注释所记的那段历史），
 * 且两者的结果形态确实不同（这里是 3 态，SMS 那边是 4 态 + 5 个 ARGV）。
 * 出现第三个 Redis+Lua 组件时再抽公共基类。
 */
@Slf4j
@Component
public class RedisRateLimiter {

    /**
     * 限流脚本：Redis 分布式令牌桶
     * 加载方式与 CacheClient / SmsRateLimiter 一致
     */
    private static final DefaultRedisScript<Long> RATE_LIMIT_SCRIPT;

    static {
        RATE_LIMIT_SCRIPT = new DefaultRedisScript<>();
        RATE_LIMIT_SCRIPT.setLocation(new ClassPathResource("rate-limit.lua"));
        RATE_LIMIT_SCRIPT.setResultType(Long.class);
    }

    /**
     * 桶容量 = qps × 该值。
     * 1.0 是为了对齐 Guava SmoothBursty 的默认 maxBurstSeconds，
     * 保证换成 Redis 实现后单实例行为不变（压测报告场景 A 的数字才能复现）。
     */
    private static final double MAX_BURST_SECONDS = 1.0;

    /** 同类告警的最小间隔，避免 Redis 故障期间每个请求打一条日志 */
    private static final long WARN_INTERVAL_MS = 30_000L;

    /** 连续失败多少次后触发本地熔断 */
    private static final int BREAKER_THRESHOLD = 3;

    /** 本地熔断的持续时间 */
    private static final long BREAKER_OPEN_MS = 10_000L;

    /**
     * 脚本执行缝 —— 把 "怎么调 Redis" 和 "怎么判结果" 分开，
     * 使单元测试无需 Spring 上下文、也无需 Mockito 去匹配
     * {@code execute(script, keys, Object...)} 那个脆弱的变长参数签名。
     */
    @FunctionalInterface
    interface ScriptRunner {
        Long run(List<String> keys, String... argv);
    }

    private final ScriptRunner scriptRunner;

    private final AtomicLong lastWarnAt = new AtomicLong(0L);

    // ---- 本地熔断状态 ----
    // 目的：Redis 故障期间，不要让每个请求都去付一次超时代价。
    // 连续失败 BREAKER_THRESHOLD 次后开闸，接下来 BREAKER_OPEN_MS 内直接
    // 本地返回 REDIS_UNAVAILABLE，一次网络调用都不发。成功一次即复位。
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private volatile long skipUntilMs = 0L;
    /** 脚本类错误的首次告警闩，保证它不会被节流淹没（见 reportScriptError） */
    private final AtomicBoolean scriptErrorReported = new AtomicBoolean(false);

    @Autowired
    public RedisRateLimiter(StringRedisTemplate stringRedisTemplate) {
        this((keys, argv) -> stringRedisTemplate.execute(RATE_LIMIT_SCRIPT, keys, (Object[]) argv));
    }

    /** 包级私有，供单元测试注入替身，无需 Spring 上下文 */
    RedisRateLimiter(ScriptRunner scriptRunner) {
        this.scriptRunner = scriptRunner;
    }

    /**
     * 尝试获取一个令牌。
     *
     * @param methodKey 方法标识（全限定类名.方法名），会拼上 {@link RedisConstants#RATE_LIMIT_API_KEY}
     * @param qps       注解上的目标速率，必须 &gt;= 1；如果 Redis 里配了
     *                  {@link RedisConstants#RATE_LIMIT_RULE_KEY} 规则，实际生效的是规则值
     */
    public Outcome tryAcquire(String methodKey, double qps) {
        // 【为什么直接抛而不是拒绝】qps < 1 时桶容量 < 1，令牌永远凑不够一个，
        // 等于静默打死这个接口。Guava 下也是同样的结果（maxPermits=0.5，
        // 永远拿不到令牌）。宁可首次请求就大声报 500，也不要变成查不出的死接口。
        if (qps < 1.0) {
            throw new IllegalStateException(
                    "@RateLimit 配置非法: " + methodKey + " 的 qps=" + qps + "，必须 >= 1");
        }

        // 本地熔断中：直接返回，不碰 Redis
        if (System.currentTimeMillis() < skipUntilMs) {
            warnThrottled("限流器处于本地熔断中，直接放行（不访问 Redis）", null);
            return Outcome.REDIS_UNAVAILABLE;
        }

        String[] argv = {
                String.valueOf(qps * MAX_BURST_SECONDS),   // capacity
                String.valueOf(qps),                       // rate
        };

        try {
            // KEYS[1]=桶，KEYS[2]=运行期阈值覆盖（见 rate-limit.lua 的 0. 步）。
            // 注解上的 qps 因此退化为「这条规则不存在时的默认值」。
            Long code = scriptRunner.run(
                    Arrays.asList(RedisConstants.RATE_LIMIT_API_KEY + methodKey,
                            DynamicConfig.rateLimitRuleKey(methodKey)), argv);

            if (code == null) {
                warnThrottled("限流脚本返回 null，本次判定为不可用", null);
                return Outcome.REDIS_UNAVAILABLE;
            }

            // 调用成功，复位熔断计数
            consecutiveFailures.set(0);
            skipUntilMs = 0L;
            scriptErrorReported.set(false);

            switch (code.intValue()) {
                case 1:
                    return Outcome.ALLOWED;
                case 0:
                    return Outcome.REJECTED;
                default:
                    warnThrottled("限流脚本返回未知码 " + code + "，本次判定为不可用", null);
                    return Outcome.REDIS_UNAVAILABLE;
            }
        } catch (DataAccessException e) {
            return onRedisFailure(e, methodKey);
        }
    }

    /** 限流判定结果。注意其中不含"要不要放行"——那是调用方根据 failOpen 决定的策略。 */
    public enum Outcome {
        /** 拿到令牌 */
        ALLOWED,
        /** 桶空，被限流 */
        REJECTED,
        /** Redis 不可用，未能判定 */
        REDIS_UNAVAILABLE
    }

    /**
     * Redis 调用失败的统一处理：区分「临时故障」和「脚本类错误」，累计熔断计数。
     */
    private Outcome onRedisFailure(DataAccessException e, String methodKey) {
        // 【关键】只捕获 DataAccessException（连接失败、超时、命令执行失败都会被
        // Spring 翻译成它）。不要捕获 RuntimeException —— ARGV 类型错误之类的
        // 编程 bug 会抛 ClassCastException，一旦被吞掉，限流器就永久静默失效，
        // 而且日志看起来和正常的降级路径一模一样。编程错误必须大声炸出来。
        if (isScriptError(e)) {
            reportScriptError(e);
        } else {
            warnThrottled("限流 Redis 调用失败，本次判定为不可用: " + methodKey, e);
        }

        if (consecutiveFailures.incrementAndGet() >= BREAKER_THRESHOLD) {
            skipUntilMs = System.currentTimeMillis() + BREAKER_OPEN_MS;
        }
        return Outcome.REDIS_UNAVAILABLE;
    }

    /**
     * 判断是不是「Lua 脚本本身出错」。
     *
     * <p>
     * 【为什么这条很重要】脚本类错误（语法错、参数为 nil、拿 false 做算术…）
     * 抛的是 {@link RedisSystemException}，而它 <strong>也是 DataAccessException</strong>，
     * 会被上面的窄捕获当成「Redis 临时挂了」放行 —— 但它是<strong>永久性</strong>的：
     * 每次调用都失败、限流器从此形同虚设，而日志和一次瞬时抖动长得一模一样。
     * 上一轮我为 SmsRateLimiter 立的规矩「只捕获 DataAccessException 让编程错误
     * 炸出来」只防住了 Java 侧的 bug，防不住 Lua 侧的。所以这里补一道显式识别。
     */
    private static boolean isScriptError(DataAccessException e) {
        if (!(e instanceof RedisSystemException)) {
            return false;
        }
        String msg = e.getMessage();
        return msg != null && msg.toLowerCase().contains("script");
    }

    /**
     * 脚本错误首次出现时用 ERROR 且不节流，确保它不会被 30 秒节流规则淹没。
     *
     * <p>
     * 【为什么这里仍然是放行而不是失败关闭】同一个 RedisSystemException 也覆盖
     * {@code MISCONF}（Redis 无法落盘时会拒绝<strong>写</strong>命令，而本脚本要 HSET）——
     * Redis 所在机器磁盘满会让全部接口持续 500，那比放行糟得多，而且更常见。
     * 所以策略不变，只是把信号放大到不可能被忽略。
     */
    private void reportScriptError(DataAccessException e) {
        if (scriptErrorReported.compareAndSet(false, true)) {
            log.error("限流 Lua 脚本执行出错！脚本类错误不会自愈，限流器将持续放行 —— "
                    + "请检查 rate-limit.lua（这是与「Redis 临时故障」完全不同的问题）：", e);
        } else {
            warnThrottled("限流 Lua 脚本仍然报错", e);
        }
    }

    /** 按间隔节流的告警。Redis 故障期间不会每个请求都刷一条日志。 */
    private void warnThrottled(String message, Exception e) {
        long now = System.currentTimeMillis();
        long last = lastWarnAt.get();
        if (now - last < WARN_INTERVAL_MS || !lastWarnAt.compareAndSet(last, now)) {
            return;
        }
        if (e == null) {
            log.warn("{}（同类告警 {} 秒内只打一条）", message, WARN_INTERVAL_MS / 1000);
        } else {
            log.warn("{}（同类告警 {} 秒内只打一条）", message, WARN_INTERVAL_MS / 1000, e);
        }
    }
}
