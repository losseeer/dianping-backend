package com.hmdp.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 短信验证码限流器 —— Redis + Lua 的分布式计数限流
 *
 * <p>
 * 【八股：为什么不用 @RateLimit 那个 Guava 令牌桶？】
 * 1. 维度不对：Guava 限的是"全站每秒 N 次"，意味着 2 个不同用户在同一个
 *    冲突、5 个攻击者就能让全站用户收不到验证码。这个接口要限的是"同一个
 *    手机号发得太快"，维度是手机号，不是全站。
 * 2. 单机内存态：Guava RateLimiter 存在 JVM 进程内。部署 N 个实例，实际
 *    阈值就是 N 倍。Redis 天然分布式，阈值才是真的阈值。
 * 3. 挡不住手机号枚举：验证码接口的真实攻击是"用大量不同手机号轰炸"
 *    （短信泵送，按条计费）。按号码限流对每个新号码都是首次发送，完全
 *    挡不住——所以这里额外加了一道全局兜底闸门。
 *
 * <p>
 * 【八股：为什么要用 Lua 脚本？】
 * "查冷却键" 和 "写冷却键" 分两次调用就是经典的 check-then-act 竞态：
 * 两个并发请求可能都查到"不存在"，然后都写入、都放行。Lua 在 Redis 里
 * 串行执行，把多个步骤焊成一个原子整体。详见 {@code sms-rate-limit.lua}。
 *
 * <p>
 * 【八股：为什么这里 Redis 挂了要放行（fail-open），而秒杀那里是失败关闭？】
 * 两个接口的代价不对称：
 *  - 验证码：Redis 挂 → 拒绝所有请求 → 全站用户登录不了。这是可用性事故。
 *    放行的代价只是"这段时间暂时失去防刷"，而且验证码本身还有 2 分钟 TTL
 *    和一次一用的约束兜着。
 *  - 秒杀：Redis 挂 → 放行 → 直接超卖。这是资金/数据事故，必须失败关闭。
 * 判断依据是"降级的代价是丢钱还是丢防护"，不是无脑选一边。
 *
 * <p>
 * 【八股：为什么用滚动窗口而不是自然日？】
 * 自然日需要算"距离下一个零点还有多少秒"，就得选一个业务时区——而运行
 * 环境的时区未必是业务时区（容器里通常是 UTC，那"今天"会在早上 8 点结束）。
 * 滚动窗口没有时区问题，而且对防刷更严：自然日允许攻击者在 23:59 和 00:01
 * 各发 10 条（2 分钟内 20 条），滚动窗口不会。
 */
@Slf4j
@Component
public class SmsRateLimiter {

    /**
     * 限流脚本：三道闸门（全局 / 日 / 冷却）一次原子判定
     * 加载方式与 CacheClient 的 UNLOCK_SCRIPT 一致
     */
    private static final DefaultRedisScript<Long> SMS_RATE_LIMIT_SCRIPT;

    static {
        SMS_RATE_LIMIT_SCRIPT = new DefaultRedisScript<>();
        SMS_RATE_LIMIT_SCRIPT.setLocation(new ClassPathResource("sms-rate-limit.lua"));
        SMS_RATE_LIMIT_SCRIPT.setResultType(Long.class);
    }

    /** 同类告警的最小间隔，避免 Redis 故障期间每个请求打一条 WARN 把日志刷爆 */
    private static final long WARN_INTERVAL_MS = 30_000L;

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

    /**
     * 预拼好的 ARGV，等价于 ARGV[1..5]。
     * 【关键】必须是 String：StringRedisTemplate 用 StringRedisSerializer
     * 序列化参数，传 int 会抛 ClassCastException。
     */
    private final String[] argv;

    private final AtomicLong lastWarnAt = new AtomicLong(0L);

    /**
     * 生产构造器。
     *
     * 【为什么每个参数都要校验 >= 1？】
     * 两个静默失效的坑，都不会报错、只会让限流悄悄失灵：
     *  1. {@code EXPIRE key 0} 在 Redis 里是**删除键**。日窗口配成 0，日计数
     *     每次发送都被清空，每日上限永远不触发。
     *  2. {@code SET key val NX EX 0} 是 Redis 语法错误，会抛异常 → 走
     *     fail-open → 冷却闸门完全失效。
     * 宁可启动就失败，也不要"看起来在跑、实际没限流"。
     */
    @Autowired
    public SmsRateLimiter(StringRedisTemplate stringRedisTemplate,
                          @Value("${sms.rate-limit.cooldown-seconds:60}") int cooldownSeconds,
                          @Value("${sms.rate-limit.daily-limit:10}") int dailyLimit,
                          @Value("${sms.rate-limit.daily-window-seconds:86400}") int dailyWindowSeconds,
                          @Value("${sms.rate-limit.global-limit:60}") int globalLimit,
                          @Value("${sms.rate-limit.global-window-seconds:60}") int globalWindowSeconds) {
        this((keys, argv) -> stringRedisTemplate.execute(SMS_RATE_LIMIT_SCRIPT, keys, (Object[]) argv),
                cooldownSeconds, dailyLimit, dailyWindowSeconds, globalLimit, globalWindowSeconds);
    }

    /** 包级私有，供单元测试注入替身，无需 Spring 上下文 */
    SmsRateLimiter(ScriptRunner scriptRunner,
                   int cooldownSeconds,
                   int dailyLimit,
                   int dailyWindowSeconds,
                   int globalLimit,
                   int globalWindowSeconds) {
        this.scriptRunner = scriptRunner;
        this.argv = new String[]{
                String.valueOf(requirePositive("cooldown-seconds", cooldownSeconds)),
                String.valueOf(requirePositive("daily-limit", dailyLimit)),
                String.valueOf(requirePositive("daily-window-seconds", dailyWindowSeconds)),
                String.valueOf(requirePositive("global-limit", globalLimit)),
                String.valueOf(requirePositive("global-window-seconds", globalWindowSeconds)),
        };
    }

    /**
     * 尝试获取一次发送许可。
     *
     * @param phone 已通过格式校验的手机号（调用方必须先校验，否则键空间会被垃圾值撑爆）
     * @return 判定结果，调用方据此决定放行或返回哪种错误提示
     */
    public Outcome tryAcquire(String phone) {
        List<String> keys = Arrays.asList(
                RedisConstants.SMS_LIMIT_COOLDOWN_KEY + phone,
                RedisConstants.SMS_LIMIT_DAILY_KEY + phone,
                RedisConstants.SMS_LIMIT_GLOBAL_KEY
        );

        try {
            Long code = scriptRunner.run(keys, argv);

            // execute 可能返回 null（脚本返回 nil）；未知码则说明脚本被改过而 Java 没跟上。
            // 两者都按"判定不了"处理 —— 与 VoucherOrderServiceImpl 对未知码失败关闭的策略
            // 相反，此处刻意选择放行，理由见类注释的 fail-open 说明。
            if (code == null) {
                warnThrottled("限流脚本返回 null，本次放行", null);
                return Outcome.REDIS_UNAVAILABLE;
            }

            switch (code.intValue()) {
                case 0:
                    return Outcome.ALLOWED;
                case 1:
                    return Outcome.COOLDOWN;
                case 2:
                    return Outcome.DAILY_EXCEEDED;
                case 3:
                    return Outcome.GLOBAL_EXCEEDED;
                default:
                    warnThrottled("限流脚本返回未知码 " + code + "，本次放行", null);
                    return Outcome.REDIS_UNAVAILABLE;
            }
        } catch (DataAccessException e) {
            // 只捕获 DataAccessException（Redis 连接/命令失败都会被 Spring 翻译成它）。
            // 【关键】不要捕获 RuntimeException：ARGV 类型错误之类的编程 bug 会抛
            // ClassCastException，一旦被吞掉，限流器就永久静默失效，而且日志看起来
            // 和正常的降级路径一模一样。编程错误必须大声炸出来。
            warnThrottled("限流 Redis 调用失败，本次放行（fail-open）: " + maskPhone(phone), e);
            return Outcome.REDIS_UNAVAILABLE;
        }
    }

    /** 限流判定结果 */
    public enum Outcome {
        /** 放行 */
        ALLOWED,
        /** 同一手机号冷却中 */
        COOLDOWN,
        /** 同一手机号超日上限 */
        DAILY_EXCEEDED,
        /** 全站超全局上限 */
        GLOBAL_EXCEEDED,
        /** Redis 不可用，已按 fail-open 放行 */
        REDIS_UNAVAILABLE
    }

    private static int requirePositive(String name, int value) {
        if (value < 1) {
            throw new IllegalStateException(
                    "短信限流配置非法: sms.rate-limit." + name + "=" + value + "，必须 >= 1");
        }
        return value;
    }

    /**
     * 按间隔节流的告警。Redis 故障期间不会每个请求都刷一条日志。
     */
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

    /**
     * 手机号脱敏。防御性处理：长度不足时原样返回，避免在 catch 块里
     * 再抛一次 StringIndexOutOfBounds —— 那会把降级路径变成异常路径。
     */
    private static String maskPhone(String phone) {
        if (phone == null || phone.length() < 11) {
            return String.valueOf(phone);
        }
        return phone.substring(0, 3) + "****" + phone.substring(7);
    }
}
