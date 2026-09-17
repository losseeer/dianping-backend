package com.hmdp.utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * SmsRateLimiter 的 Lua 脚本集成测试 —— 需要本地 Redis。
 *
 * <p>
 * 这是**唯一能验证 sms-rate-limit.lua 本身**的测试：单测只能验证 Java 侧的返回值
 * 映射，验证不了脚本里三道闸门的真实判定逻辑。本地起 Redis 后去掉 @Disabled 跑。
 *
 * <p>
 * 【跑之前必读的坑】日上限的检查排在冷却检查之前。发送次数达到日上限后，
 * **即使删掉冷却键，脚本依然返回 DAILY_EXCEEDED** —— 这是刻意设计（避免已达
 * 上限的用户白白烧掉一次冷却），不是 bug。所以如果要继续做别的断言，必须把
 * 日计数键一起 DEL 掉，只删冷却键会让你误判脚本坏了。
 */
@SpringBootTest
@Disabled("requires local Redis；手工验证 Lua 脚本用，CI 不跑")
@DisplayName("SmsRateLimiter 的 Lua 脚本（需本地 Redis）")
class SmsRateLimiterLuaTest {

    private static final String TEST_PHONE = "13900000001";
    private static final String COOLDOWN_KEY = RedisConstants.SMS_LIMIT_COOLDOWN_KEY + TEST_PHONE;
    private static final String DAILY_KEY = RedisConstants.SMS_LIMIT_DAILY_KEY + TEST_PHONE;

    @Resource
    private SmsRateLimiter smsRateLimiter;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Value("${sms.rate-limit.daily-limit}")
    private int dailyLimit;

    @BeforeEach
    @AfterEach
    void cleanKeys() {
        stringRedisTemplate.delete(COOLDOWN_KEY);
        stringRedisTemplate.delete(DAILY_KEY);
        // 全局计数键也必须清 —— 本测试每次都会给它 +3 左右，而它只有一个
        // 60 秒的滚动窗口。不清的话，在窗口内反复跑本测试会把它顶到上限
        // （默认 60），之后测试就随机拿到 GLOBAL_EXCEEDED 而不是 ALLOWED，
        // 变成典型的 flaky 测试。实测跑几轮后就会复现。
        // 代价是会短暂扰动这个共享计数键；对 @Disabled 的手工测试可接受。
        stringRedisTemplate.delete(RedisConstants.SMS_LIMIT_GLOBAL_KEY);
    }

    @Test
    @DisplayName("冷却闸门：首次放行 → 立即重发被拒 → 删掉冷却键后可再次放行")
    void cooldownGate() {
        assertEquals(SmsRateLimiter.Outcome.ALLOWED, smsRateLimiter.tryAcquire(TEST_PHONE));
        assertEquals(SmsRateLimiter.Outcome.COOLDOWN, smsRateLimiter.tryAcquire(TEST_PHONE));

        stringRedisTemplate.delete(COOLDOWN_KEY);
        assertEquals(SmsRateLimiter.Outcome.ALLOWED, smsRateLimiter.tryAcquire(TEST_PHONE));
    }

    @Test
    @DisplayName("冷却键的 TTL 与配置一致，且被拒的请求不会消耗冷却")
    void cooldownTtlIsSetAndRejectionDoesNotConsumeIt() {
        assertEquals(SmsRateLimiter.Outcome.ALLOWED, smsRateLimiter.tryAcquire(TEST_PHONE));

        Long ttl = stringRedisTemplate.getExpire(COOLDOWN_KEY);
        // 取的是 60 秒冷却，执行有耗时，允许几秒误差
        assertEquals(true, ttl != null && ttl > 0 && ttl <= 60,
                "冷却键 TTL 应在 (0, 60] 秒内，实际: " + ttl);

        // 被拒的请求不应让日计数变少（计数语义是"成功发送次数"，不是"请求次数"）
        String before = stringRedisTemplate.opsForValue().get(DAILY_KEY);
        smsRateLimiter.tryAcquire(TEST_PHONE);
        assertEquals(before, stringRedisTemplate.opsForValue().get(DAILY_KEY));
    }

    @Test
    @DisplayName("日上限闸门：连发到上限后持续返回超限")
    void dailyGate() {
        for (int i = 1; i <= dailyLimit; i++) {
            // 每次先清冷却键，才能连续累加日计数
            stringRedisTemplate.delete(COOLDOWN_KEY);
            assertEquals(SmsRateLimiter.Outcome.ALLOWED, smsRateLimiter.tryAcquire(TEST_PHONE),
                    "第 " + i + " 次（未超上限）应放行");
        }

        stringRedisTemplate.delete(COOLDOWN_KEY);
        assertEquals(SmsRateLimiter.Outcome.DAILY_EXCEEDED, smsRateLimiter.tryAcquire(TEST_PHONE),
                "已达日上限，即便冷却已清空也应被拒");

        // 日计数应与上限严格相等 —— 被拒的请求没有让它继续涨
        assertEquals(String.valueOf(dailyLimit), stringRedisTemplate.opsForValue().get(DAILY_KEY));
    }

    // 全局闸门未在此覆盖：它需要打满 global-limit 次（默认 60）才能触发，而这些发送会
    // 污染全局计数、影响同批次的其它测试。逻辑与日计数闸门同构，风险低于收益，略过。
}
