package com.hmdp.controller;

import com.hmdp.annotation.RateLimit;
import com.hmdp.aspect.RateLimitAspect;
import com.hmdp.dto.Result;
import com.hmdp.listener.TransactionOutboxPublisher;
import com.hmdp.utils.DynamicConfig;
import com.hmdp.utils.RedisConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 运行期配置管理接口 —— 不改代码、不重启，改限流阈值和 Outbox 扫描间隔
 *
 * <p>
 * 【八股：为什么「配置放在 Redis」而不是「配置放在 application.yaml」？】
 * yaml 的值在启动时绑定进 Bean，改一次要重启全部实例；重启又要重新预热缓存、
 * 重新预热 JIT，为一个开关付一次全量发布。更重要的是集群语义：滚动发布期间
 * 新旧配置并存，改阈值这件事本身就没有一致答案。Redis 是所有实例共享的一份状态，
 * 写一次全部生效 —— 和令牌桶状态搬进 Redis 是同一个理由。
 *
 * <p>
 * 【八股：为什么阈值覆盖读在 Lua 里，而不是 Java 里读完再传进 ARGV？】
 * 限流判定是热路径，Java 侧先 GET 再 EVAL 等于每个请求多一个 RTT；
 * 更关键的是「读到的规则」和「写回的桶」之间出现了窗口，改规则的瞬间
 * 各实例可能各按新旧两套 CAP/RATE 补同一个桶。见 {@code rate-limit.lua} 第 0 步。
 *
 * <p>
 * 【为什么只给「覆盖值」开口子，不给「默认值」开口子】
 * 注解上的 qps 仍然是代码的一部分、走 code review、跟着版本回滚。
 * 这里写的值只是临时覆盖，删掉键就回到注解值 —— 存在一个「运维改过的值比代码里的大
 * 十倍，三个月后没人记得」的风险，所以每个响应都回显 override 与 effective 两个字段，
 * {@code GET /config} 也能一眼看出哪些接口正处于覆盖状态。
 *
 * <p>
 * 【鉴权口径 —— 已知缺陷，与 {@code MvcConfig} 里记的那条同源】
 * {@code /config/**} 不在登录拦截器白名单里，所以未登录调不到；
 * 但<strong>任何登录用户</strong>都能改全站限流阈值，没有角色校验。
 * 本项目没有管理员角色体系，这里就不假装有：要接的话在 LoginInterceptor 之后
 * 加一个角色拦截器即可，切换点就一处。生产环境至少要在网关上按内网收一遍。
 */
@Slf4j
@RestController
@RequestMapping("/config")
public class ConfigController {

    /**
     * api 的形状：至少两段点分标识符，即 {@code 全限定类名.方法名}。
     *
     * 【为什么要卡形状】api 会被拼进 Redis 键名。放 {{}} 进去就能造出两个 hash tag、
     * 放进 * 就能造出一个 SCAN 模式键，都是「写进去没事、过很久在别处炸掉」的问题。
     * 与其在读取侧兜，不如在唯一的写入口就拒绝。
     *
     * 【挡不住的那一半：容器先归一化，我们再校验】实测 {@code ?api=a{b}.c} 到这里
     * 已经变成 {@code ab.c}（花括号被 Tomcat 按 query 规则吃掉了），所以校验看到的是
     * 归一化之后的串。这就是响应里必须回显 api 的原因 —— 那才是真正写进去的键，
     * 和运维手打的那串不一样时，他自己一眼就能看出来（正确做法是调用方做 URL 编码）。
     */
    private static final Pattern API_PATTERN =
            Pattern.compile("^[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)+$");

    /** 键名长度上限：防止有人拿一个 100KB 的 api 往 Redis 里塞一个巨型键 */
    private static final int MAX_API_LENGTH = 160;

    @Resource
    private DynamicConfig dynamicConfig;
    @Resource
    private RateLimitAspect rateLimitAspect;
    @Resource
    private TransactionOutboxPublisher outboxPublisher;

    /**
     * 总览：当前每一项动态配置的「默认值 / 覆盖值 / 实际生效值」。
     *
     * 【这个接口的全部意义就是这三列能对齐】只看覆盖值会漏掉「没配过 = 用注解默认值」，
     * 只看生效值会看不出它是不是被覆盖过 —— 半夜被叫起来的人需要一眼分清这两种情况。
     */
    @GetMapping
    @RateLimit(qps = 10, message = "配置查询过于频繁，请稍后再试")
    public Result list() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("rateLimit", rateLimitEntries());
        data.put("outbox", outboxEntry());
        return Result.ok(data);
    }

    /**
     * 单个接口的限流配置。api 走 query 参数而不是路径变量：
     * 值里全是点（{@code com.hmdp.controller.XxxController.method}），
     * 放在路径里会被 Spring MVC 的后缀模式匹配截掉最后一段，是一种只在特定配置下才出现的坑。
     *
     * 【返回单个对象而不是数组】和 PUT/DELETE 的回显保持同一形状 —— 三个端点操作的就是同一条配置。
     */
    @GetMapping("/ratelimit")
    @RateLimit(qps = 10, message = "配置查询过于频繁，请稍后再试")
    public Result getRateLimit(@RequestParam("api") String api) {
        Result invalid = checkApi(api);
        if (invalid != null) {
            return invalid;
        }
        Double annotationDefault = rateLimitAspect.annotationDefaults().get(api);
        if (annotationDefault == null && dynamicConfig.raw(DynamicConfig.rateLimitRuleKey(api)) == null) {
            // 既没配过覆盖、也不在登记表里 = 问了一个这里管不着的接口，
            // 回显一行两个 null 会被读成"配置是空的但生效值待定"。
            return Result.fail("未配置该接口的规则，且它不在登记表里（启动后未被调用过？）: " + api);
        }
        return Result.ok(rateLimitEntry(api, annotationDefault));
    }

    /**
     * 覆盖某个接口的 qps，对所有实例立刻生效（下一个请求就读到新值）。
     *
     * 【非法值为什么必须在写入口拒绝】rate-limit.lua 对键里的非法值是「忽略、继续用注解值」，
     * 不报错。所以写入口是唯一能把错误告诉人的地方 —— 放过去的话，
     * 运维会以为自己改了，实际什么都没发生。
     */
    @PutMapping("/ratelimit")
    @RateLimit(qps = 10, message = "配置修改过于频繁，请稍后再试")
    public Result putRateLimit(@RequestParam("api") String api, @RequestParam("qps") Double qps) {
        Result invalid = checkApi(api);
        if (invalid != null) {
            return invalid;
        }
        if (qps == null) {
            return Result.fail("缺少 qps 参数");
        }
        if (qps < DynamicConfig.MIN_QPS || qps > DynamicConfig.MAX_QPS) {
            return Result.fail(String.format("qps 必须在 [%s, %s] 之间，收到: %s。"
                            + "下限的理由：qps < 1 时桶容量不足一个令牌，等于静默打死这个接口。",
                    DynamicConfig.MIN_QPS, DynamicConfig.MAX_QPS, qps));
        }

        String ruleKey = DynamicConfig.rateLimitRuleKey(api);
        try {
            dynamicConfig.put(ruleKey, String.valueOf(qps));
        } catch (Exception e) {
            // 写失败必须大声：这里不能像读取侧那样"回退到默认值"，
            // 否则调用方拿到的 200 会在撒谎。
            log.error("写入限流规则失败: key={}", ruleKey, e);
            return Result.fail("写入失败（Redis 不可用？）: " + e.getMessage());
        }

        Double annotationDefault = rateLimitAspect.annotationDefaults().get(api);
        Map<String, Object> echo = rateLimitEntry(api, annotationDefault);
        log.warn("限流阈值已动态覆盖: api={}, qps={}（注解默认值={}），全部实例下一个请求起生效",
                api, qps, annotationDefault);
        return Result.ok(echo);
    }

    /**
     * 删除覆盖，回到注解默认值。
     * 【为什么删除是「回到默认」而不是「回到 0」】规则键只被读取、不被任何写入方回填，
     * 见 {@link RedisConstants#DYNAMIC_CONFIG_KEY} 那条铁律。
     */
    @DeleteMapping("/ratelimit")
    @RateLimit(qps = 10, message = "配置修改过于频繁，请稍后再试")
    public Result deleteRateLimit(@RequestParam("api") String api) {
        Result invalid = checkApi(api);
        if (invalid != null) {
            return invalid;
        }
        dynamicConfig.remove(DynamicConfig.rateLimitRuleKey(api));
        Map<String, Object> echo = rateLimitEntry(api, rateLimitAspect.annotationDefaults().get(api));
        log.warn("限流阈值覆盖已清除，回到注解默认值: api={}, effective={}",
                api, echo.get("effectiveQps"));
        return Result.ok(echo);
    }

    /** 调整 Outbox 发布器的扫描间隔（毫秒），立刻生效于下一轮 tick 的门控判断 */
    @PutMapping("/outbox-interval")
    @RateLimit(qps = 10, message = "配置修改过于频繁，请稍后再试")
    public Result putOutboxInterval(@RequestParam("ms") Long ms) {
        if (ms == null) {
            return Result.fail("缺少 ms 参数");
        }
        if (ms < DynamicConfig.MIN_INTERVAL_MS || ms > DynamicConfig.MAX_INTERVAL_MS) {
            return Result.fail(String.format("扫描间隔必须在 [%d, %d] 毫秒之间，收到: %d。"
                            + "上限的理由见 DynamicConfig#MAX_INTERVAL_MS（再长就等于暂停投递，且不报错）。",
                    DynamicConfig.MIN_INTERVAL_MS, DynamicConfig.MAX_INTERVAL_MS, ms));
        }
        try {
            dynamicConfig.put(RedisConstants.OUTBOX_PUBLISH_INTERVAL_KEY, String.valueOf(ms));
        } catch (Exception e) {
            log.error("写入 Outbox 扫描间隔失败", e);
            return Result.fail("写入失败（Redis 不可用？）: " + e.getMessage());
        }
        log.warn("Outbox 扫描间隔已动态调整为 {}ms（下一轮 tick 起生效）", ms);
        return Result.ok(outboxEntry());
    }

    /** 清除间隔覆盖，回到 {@code transaction.outbox.publish-interval-ms} */
    @DeleteMapping("/outbox-interval")
    @RateLimit(qps = 10, message = "配置修改过于频繁，请稍后再试")
    public Result deleteOutboxInterval() {
        dynamicConfig.remove(RedisConstants.OUTBOX_PUBLISH_INTERVAL_KEY);
        log.warn("Outbox 扫描间隔覆盖已清除，回到配置文件默认值");
        return Result.ok(outboxEntry());
    }

    // ---------------------------------------------------------------- 组装

    /** 总览里的那张表：登记表里的接口 + 只有覆盖键的幽灵接口 */
    private List<Map<String, Object>> rateLimitEntries() {
        Map<String, Double> defaults = rateLimitAspect.annotationDefaults();
        // 登记表是 ConcurrentHashMap，本身没有稳定顺序；不排序的话两次快照的行序会变，
        // 想 diff 出"到底哪一行被改了"就只能靠肉眼。
        List<String> known = new ArrayList<>(defaults.keySet());
        Collections.sort(known);
        Set<String> apis = new LinkedHashSet<>(known);
        // 再补上「只有覆盖键、但登记表里没有」的接口：可能是从没用过的接口被 redis-cli 配过，
        // 也可能是删掉的代码留下的残键。两种都得看得见，不然就成了幽灵配置。
        for (String key : dynamicConfig.keysWithPrefix(RedisConstants.RATE_LIMIT_RULE_KEY)) {
            apis.add(key.substring(RedisConstants.RATE_LIMIT_RULE_KEY.length()));
        }

        List<Map<String, Object>> list = new ArrayList<>();
        for (String api : apis) {
            list.add(rateLimitEntry(api, defaults.get(api)));
        }
        return list;
    }

    private Map<String, Object> rateLimitEntry(String api, Double annotationDefault) {
        String raw = dynamicConfig.raw(DynamicConfig.rateLimitRuleKey(api));

        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("api", api);
        entry.put("annotationQps", annotationDefault);
        entry.put("override", raw);
        entry.put("effectiveQps", effectiveQps(raw, annotationDefault));
        entry.put("overridden", raw != null);
        if (annotationDefault == null) {
            entry.put("note", "该 api 不在登记表里（启动后没被调用过 / 只被 redis-cli 配过），"
                    + "注解默认值未知，effective 仅按覆盖值计算");
        }
        return entry;
    }

    /**
     * 实际生效值 —— 判定口径必须和 rate-limit.lua 第 0 步逐字一致：
     * 覆盖值能解析成数字且 &gt;= 1 就用它，否则用注解值。
     *
     * 【为什么连「超出 MAX_QPS 也照样生效」都照抄】写入接口挡了越界，
     * 但直接 redis-cli 塞进来的值 Lua 只认 >= 1。这里若另立一套更严的口径，
     * 接口报的「生效值」就和真实行为不符 —— 而这是唯一一处人能看到生效值的地方。
     * 越界不改变 effective，只额外留一句提示。
     */
    private static Object effectiveQps(String raw, Double annotationDefault) {
        if (raw != null) {
            try {
                double override = Double.parseDouble(raw.trim());
                if (override >= DynamicConfig.MIN_QPS) {
                    return override;
                }
            } catch (NumberFormatException ignored) {
                // 落到这里就是「脚本也会忽略它」，按注解值报
            }
        }
        return annotationDefault;
    }

    private Map<String, Object> outboxEntry() {
        Map<String, Object> entry = new LinkedHashMap<>();
        long effective = outboxPublisher.effectiveIntervalMs();
        entry.put("key", RedisConstants.OUTBOX_PUBLISH_INTERVAL_KEY);
        entry.put("override", dynamicConfig.raw(RedisConstants.OUTBOX_PUBLISH_INTERVAL_KEY));
        entry.put("effectiveIntervalMs", effective);
        entry.put("allowedRangeMs",
                DynamicConfig.MIN_INTERVAL_MS + " ~ " + DynamicConfig.MAX_INTERVAL_MS);
        entry.put("schedulerTickMs", DynamicConfig.MIN_INTERVAL_MS);
        entry.put("note", "effectiveIntervalMs 已夹在合法区间内，所以它可能等于配置文件的默认值"
                + "而不是键里的原始值；两者不一致就是覆盖值非法。原始值见 override。");
        return entry;
    }

    private static Result checkApi(String api) {
        if (api == null || api.trim().isEmpty()) {
            return Result.fail("缺少 api 参数（全限定类名.方法名）");
        }
        if (api.length() > MAX_API_LENGTH) {
            return Result.fail("api 超过 " + MAX_API_LENGTH + " 字符，不会是真实的类名.方法名");
        }
        if (!API_PATTERN.matcher(api).matches()) {
            return Result.fail("api 必须是「全限定类名.方法名」（点分的合法标识符，不含空格/花括号/通配符），收到: " + api);
        }
        return null;
    }
}
