package com.hmdp.utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RedisRateLimiter 的 Lua 脚本集成测试 —— 需要本地 Redis。
 *
 * <p>
 * 这是**唯一能验证 rate-limit.lua 本身**的测试：单测只能验证 Java 侧的返回值
 * 映射，验证不了令牌怎么补、上限怎么夹、TTL 设了没有。本地起 Redis 后去掉
 * @Disabled 跑：
 * <pre>
 * mvn test -Dtest=RedisRateLimiterLuaTest \
 *   -Djunit.jupiter.conditions.deactivate=org.junit.jupiter.engine.extension.DisabledCondition
 * </pre>
 *
 * <p>
 * 相比 SmsRateLimiterLuaTest 的一个结构性优势：这里 qps 是方法参数而不是配置项，
 * 所以不用改注解或 yaml 就能测任意速率。
 *
 * <p>
 * 【为什么断言都写得很松】这是个基于真实时钟的限流器。凡是"精确等于某个数"的
 * 断言（比如"第 6 次一定被拒"）都会在慢机器上偶发失败。所以下面一律只断言
 * <strong>方向</strong>和<strong>上界</strong>，并留出充足余量。
 */
@SpringBootTest
@Disabled("requires local Redis；手工验证 Lua 脚本用，CI 不跑")
@DisplayName("RedisRateLimiter 的 Lua 脚本（需本地 Redis）")
class RedisRateLimiterLuaTest {

    private static final String METHOD_KEY = "com.hmdp.test.LuaProbe.method";
    private static final String BUCKET_KEY = RedisConstants.RATE_LIMIT_API_KEY + METHOD_KEY;

    @Resource
    private RedisRateLimiter redisRateLimiter;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @BeforeEach
    @AfterEach
    void cleanBucket() {
        stringRedisTemplate.delete(BUCKET_KEY);
    }

    @Test
    @DisplayName("冷桶是满的：qps=5 时能连续突发 5 次，之后被拒")
    void coldBucketIsFullThenThrottles() {
        int allowed = 0;
        for (int i = 0; i < 5; i++) {
            if (redisRateLimiter.tryAcquire(METHOD_KEY, 5) == RedisRateLimiter.Outcome.ALLOWED) {
                allowed++;
            }
        }
        // 5 次调用在几毫秒内跑完，这段耗时补充的令牌远不足 1 个（qps=5 → 200ms 才 1 个），
        // 所以除非机器卡顿超过 200ms，5 次应当全部放行
        assertTrue(allowed >= 4, "冷桶应有约 qps 个令牌可突发，实际放行 " + allowed);

        // 耗尽后必然被拒
        RedisRateLimiter.Outcome last = RedisRateLimiter.Outcome.ALLOWED;
        for (int i = 0; i < 100 && last == RedisRateLimiter.Outcome.ALLOWED; i++) {
            last = redisRateLimiter.tryAcquire(METHOD_KEY, 5);
        }
        assertEquals(RedisRateLimiter.Outcome.REJECTED, last, "令牌耗尽后必须被拒");
    }

    @Test
    @DisplayName("被拒的请求不消耗令牌（只可能因补充而增加）")
    void rejectedRequestDoesNotConsumeTokens() {
        drain(5);

        String before = (String) stringRedisTemplate.opsForHash().get(BUCKET_KEY, "tokens");
        assertEquals(RedisRateLimiter.Outcome.REJECTED, redisRateLimiter.tryAcquire(METHOD_KEY, 5));
        String after = (String) stringRedisTemplate.opsForHash().get(BUCKET_KEY, "tokens");

        assertTrue(Double.parseDouble(after) >= Double.parseDouble(before),
                "被拒时令牌数不应减少：" + before + " → " + after);
    }

    @Test
    @DisplayName("令牌会随时间补充（只断言方向，不断言精确数量）")
    void refillsOverTime() {
        drain(2);

        // qps=2 → 每秒补 2 个。等 600ms 应补上 1 个以上
        sleep(600);
        assertEquals(RedisRateLimiter.Outcome.ALLOWED, redisRateLimiter.tryAcquire(METHOD_KEY, 2));
    }

    @Test
    @DisplayName("亚秒精度：qps=100 时等 50ms 应补出令牌（秒级实现必挂）")
    void subSecondPrecision() {
        // 这条是防"把时间戳退化成秒级"的守卫。qps=100 意味着 10ms 补一个令牌；
        // 若精度只有秒，耗尽后等 50ms 仍然一个令牌都没有，这条会失败。
        drain(100);
        sleep(50);

        assertEquals(RedisRateLimiter.Outcome.ALLOWED, redisRateLimiter.tryAcquire(METHOD_KEY, 100),
                "50ms 应补出约 5 个令牌；若是秒级精度则一个都没有 —— 说明 redis.call('time') 的毫秒部分丢了");
    }

    @Test
    @DisplayName("空闲过期后再用，等同冷启动（桶是满的）")
    void expiryIsSemanticallyInvisible() {
        // TTL 是 2 秒。等它过期后键会消失（不是被手动删），下一个请求重建满桶。
        // 这与"空闲足够久后桶本来就该是满的"完全等价，所以过期在语义上不可见。
        assertEquals(RedisRateLimiter.Outcome.ALLOWED, redisRateLimiter.tryAcquire(METHOD_KEY, 5));
        stringRedisTemplate.delete(BUCKET_KEY);   // 模拟过期

        assertEquals(RedisRateLimiter.Outcome.ALLOWED, redisRateLimiter.tryAcquire(METHOD_KEY, 5));
    }

    @Test
    @DisplayName("存储结构是 Hash 且恰有 2 个字段，TTL 落在 (0,2]")
    void bucketStructureAndTtl() {
        redisRateLimiter.tryAcquire(METHOD_KEY, 5);

        assertEquals(DataType.HASH, stringRedisTemplate.type(BUCKET_KEY));
        assertEquals(2L, stringRedisTemplate.opsForHash().size(BUCKET_KEY),
                "应是 {tokens, ts} 两个字段。改成两个独立 String key 会重新引入部分过期的失败模式");

        Long ttl = stringRedisTemplate.getExpire(BUCKET_KEY);
        assertTrue(ttl != null && ttl > 0 && ttl <= 2, "TTL 应在 (0,2] 秒内，实际: " + ttl);
    }

    @Test
    @DisplayName("并发下不超发（原子性验证）")
    void doesNotOverAdmitUnderConcurrency() throws Exception {
        int threads = 32;
        int callsPerThread = 50;
        double qps = 50;

        AtomicInteger allowed = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        long startMs = System.currentTimeMillis();
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < callsPerThread; i++) {
                        if (redisRateLimiter.tryAcquire(METHOD_KEY, qps) == RedisRateLimiter.Outcome.ALLOWED) {
                            allowed.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS), "并发任务超时未完成");
        long elapsedMs = System.currentTimeMillis() - startMs;
        pool.shutdownNow();

        // 上界 = 冷桶容量 + 这段时间补充的量 + 余量。
        // 断言写松是刻意的：这条测的是"不会因为竞态超发"，而不是精确速率。
        double upperBound = qps + qps * (elapsedMs / 1000.0) + 20;
        assertTrue(allowed.get() <= upperBound,
                "放行数 " + allowed.get() + " 超过上界 " + upperBound
                        + "（耗时 " + elapsedMs + "ms）—— check-and-set 未做到原子");
    }

    // ---------------------------------------------------------------- 辅助

    /** 耗尽令牌桶：一直取到被拒为止 */
    private void drain(double qps) {
        for (int i = 0; i < 500; i++) {
            if (redisRateLimiter.tryAcquire(METHOD_KEY, qps) == RedisRateLimiter.Outcome.REJECTED) {
                return;
            }
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
