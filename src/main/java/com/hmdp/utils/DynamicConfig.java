package com.hmdp.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 运行期动态配置的读写原语 —— 见 {@link RedisConstants#DYNAMIC_CONFIG_KEY} 那一族的键约定。
 *
 * <p>
 * 【这个类只解决"读"，语义由各读取方决定】
 * <ul>
 *   <li>限流阈值：不在这里读。rate-limit.lua 把规则键当 KEYS[2] 自己取，
 *       这样每个请求不多一次 RTT，且"换阈值"和"扣令牌"落在同一个原子脚本里。
 *       本类在这里只负责写入、删除和列出来给人看。</li>
 *   <li>Outbox 扫描间隔：每轮扫描调一次 {@link #getLong}。</li>
 * </ul>
 *
 * <p>
 * 【读失败一律回退到调用方给的默认值，并且不打日志】
 * 这不是"静默吞异常"：配置读取的失败语义本来就是"没配过"，而 Redis 真挂了时，
 * 限流器会走它自己的 failOpen/failClosed 分支、扫描线程会在别的地方报错。
 * 代价要记在账上：<strong>一个写错的关键字永远不会报错</strong> —— 它只是安静地不被任何人读。
 * 所以新增一个配置项必须同时做两件事：写进 {@code ConfigController} 的清单（否则没人读得到），
 * 以及给 {@code GET /config} 一个能回显原始值的出口。
 */
@Slf4j
@Component
public class DynamicConfig {

    /** qps 下限：1。低于 1 会让桶容量不足一个令牌，等于静默打死接口（与 Java 侧对注解的校验同口径） */
    public static final double MIN_QPS = 1.0D;
    /** 上限只是防手滑（把 100 写成 100000），不是容量规划 */
    public static final double MAX_QPS = 100_000.0D;

    /**
     * 扫描间隔下限。取的是发布器调度粒度的同一个值
     * （{@code TransactionOutboxPublisher.SCAN_TICK_MS}）—— 配得比 tick 更小也没有意义，
     * 与其让「配了 50 实际跑 200」这种落差存在，不如把下限抬到真正能生效的最小值。
     */
    public static final long MIN_INTERVAL_MS = 200L;
    /**
     * 上限 10 秒。判据是告警口径：{@code dianping_outbox_oldest_age_seconds > 60} 读的是
     * 「上一轮扫描看到的积压」，间隔越长这个快照越陈旧，两者差一个数量级才不会互相误伤。
     * 比 10 秒更长就不是「给数据库减负」而是「暂停投递」了，暂停该显式地做（停实例），
     * 用「配一个巨大的间隔」来表达，监控上看到的是「一切正常、就是不干活」，比老实报错难查。
     */
    public static final long MAX_INTERVAL_MS = 10_000L;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /** 限流规则键：与 rate-limit.lua 的 KEYS[2] 同一份拼法，只允许这一处拼 */
    public static String rateLimitRuleKey(String api) {
        return RedisConstants.RATE_LIMIT_RULE_KEY + api;
    }

    /** 原始值；键不存在或 Redis 读不到都返回 null */
    public String raw(String key) {
        try {
            return stringRedisTemplate.opsForValue().get(key);
        } catch (DataAccessException e) {
            log.debug("动态配置读取失败，按未配置处理: key={}", key, e);
            return null;
        }
    }

    /**
     * 读一个整数配置；缺失、非整数、越界都回退到 fallback。
     *
     * 【为什么区间由调用方传而不是写死在这里】合法区间是「这一项配置」的语义，
     * 不是这个类的语义。写死的话，将来加一个数量级完全不同的配置项，
     * 它会撞上这里为扫描间隔定的界限，然后安静地走 fallback ——
     * 而那正是本类最不该出现的失败方式。
     */
    public long getLong(String key, long fallback, long minInclusive, long maxInclusive) {
        String raw = raw(key);
        if (raw == null) {
            return fallback;
        }
        try {
            long value = Long.parseLong(raw.trim());
            if (value < minInclusive || value > maxInclusive) {
                return fallback;
            }
            return value;
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    public void put(String key, String value) {
        stringRedisTemplate.opsForValue().set(key, value);
    }

    public void remove(String key) {
        stringRedisTemplate.delete(key);
    }

    /**
     * 列出某个前缀下已存在的配置键。
     *
     * 【为什么用 SCAN 不用 KEYS】KEYS 是 O(N) 且不分片，在 Redis 主线程上一次扫描
     * 就能把整个实例卡住 —— 而这正是我们在做的事：给运维一个随时可点的管理接口。
     *
     * 【为什么走 execute(RedisCallback) 而不是 stringRedisTemplate 上的现成方法】
     * 这个版本的 spring-data-redis 只在 {@code RedisKeyCommands} 上提供了 scan，
     * {@code RedisTemplate} 没有把它透出来（{@code RedisTemplate#keys} 倒是有，
     * 但那是 KEYS，正是要避开的那个）。所以只能拿原始连接、自己解 UTF-8。
     */
    public Set<String> keysWithPrefix(String prefix) {
        ScanOptions options = ScanOptions.scanOptions().match(prefix + "*").count(200).build();
        try {
            Set<String> keys = stringRedisTemplate.execute((RedisCallback<Set<String>>) connection -> {
                Set<String> found = new LinkedHashSet<>();
                try (Cursor<byte[]> cursor = connection.scan(options)) {
                    while (cursor.hasNext()) {
                        found.add(new String(cursor.next(), StandardCharsets.UTF_8));
                    }
                }
                return found;
            });
            return keys == null ? Collections.emptySet() : keys;
        } catch (DataAccessException e) {
            // 列举失败不影响任何判定（这些键只是用来给人看的），返回空集合即可；
            // 真正读某个键的路径是 raw()，它有自己的兜底。
            log.warn("动态配置列举失败，本次返回空: prefix={}", prefix, e);
            return Collections.emptySet();
        }
    }
}
