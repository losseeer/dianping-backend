package com.hmdp.annotation;

import java.lang.annotation.*;

/**
 * 接口限流注解 —— 基于令牌桶算法
 *
 * 【八股：令牌桶算法原理】
 * 1. 系统以固定速率往桶里放令牌（token）
 * 2. 请求来时从桶里取一个令牌，取到则放行，取不到则拒绝
 * 3. 桶满了就丢弃多余的令牌（防止令牌堆积）
 * 4. 允许一定程度的突发流量（桶里攒的令牌可以瞬间消耗完）
 *
 * 【八股：令牌桶 vs 漏桶的区别】
 * 令牌桶（本项目的 RedisRateLimiter，Redis + Lua 实现）：
 *   - 允许突发流量（桶里有令牌就可以瞬间放行多个请求）
 *   - 适合"平时低流量，偶尔高并发"的场景（如秒杀）
 *
 * 漏桶（Leaky Bucket）：
 *   - 请求匀速流出，不管来多少请求，出去的速率恒定
 *   - 适合需要严格匀速的场景（如消息队列消费）
 *
 * 【八股：常见限流算法对比】
 * | 算法     | 突发流量 | 匀速  | 复杂度 | 适用场景          |
 * |---------|---------|-------|--------|-----------------|
 * | 计数器   | 不允许   | 否    | 低     | 固定QPS限制       |
 * | 滑动窗口  | 部分允许 | 否    | 中     | 精确控制时间窗口    |
 * | 漏桶     | 不允许   | 是    | 中     | 整流（让流量均匀）  |
 * | 令牌桶   | 允许     | 否    | 中     | 允许突发的一般限流  |
 *
 * 使用示例：
 *   @RateLimit(qps = 50, fallback = "seckillFallback")
 *   public Result seckillVoucher(Long voucherId) { ... }
 *
 *   private Result seckillFallback(Long voucherId) {
 *       return Result.fail("当前排队人数较多，请稍后再试");
 *   }
 *
 * 【qps 现在是「默认值」而不是「配置」】
 * 运行期可以用 {@code PUT /config/ratelimit?api=全限定类名.方法名&qps=...} 覆盖，
 * 覆盖值写在 Redis 里、由 {@code rate-limit.lua} 在同一次原子求值里读出并当场重算容量/速率，
 * 全部实例下一个请求起生效，删掉键就回到这里的注解值。
 * 所以：注解上的数字仍然是代码（走 review、跟版本回滚），运行期的那个数字是临时覆盖。
 * 两者不一致时以 Redis 里的为准 —— {@code GET /config} 同时回显 annotationQps / override /
 * effectiveQps 三列，就是为了让人一眼看出当前到底走的哪一个。
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RateLimit {

    /**
     * 每秒允许的请求数（QPS），必须 &gt;= 1；这是「默认值」，运行期可被 Redis 里的规则覆盖（见类注释）
     * 默认100，秒杀接口建议设50，普通接口设200
     */
    double qps() default 100;

    /**
     * 降级方法名
     * 被限流时调用此方法，方法签名需与原方法一致
     * 如果不指定，默认返回"请求过于频繁，请稍后再试"
     */
    String fallback() default "";

    /**
     * 限流提示消息（fallback为空时使用）
     */
    String message() default "请求过于频繁，请稍后再试";

    /**
     * Redis 不可用时是否放行。
     *
     * <p>
     * 【八股：fail-open 还是 fail-closed？判据是"降级的代价是丢钱还是丢防护"】
     * 而不是无脑选一边：
     * <ul>
     *   <li><b>失败关闭（failOpen = false）</b>—— 适合丢钱/丢数据一致性的路径。
     *       秒杀放行会超卖、支付放行会重复扣款，宁可拒绝。
     *       代价是 Redis 一挂这些接口就不可用。</li>
     *   <li><b>放行（failOpen = true，默认）</b>—— 适合丢防护的路径。
     *       登录放行只是暂时失去防撞库、搜索放行只是暂时失去防刷，
     *       而拒绝会让全站用户登不上、搜不了。可用性优先。</li>
     * </ul>
     *
     * <p>
     * 【注意】只配了 fail-open 是不够的：客户端必须有超时，否则请求会挂住
     * 而不是快速降级。见 application.yaml 的 spring.redis.timeout 与
     * lettuce.pool.max-wait，以及 RedisRateLimiter 里的本地熔断。
     */
    boolean failOpen() default true;
}
