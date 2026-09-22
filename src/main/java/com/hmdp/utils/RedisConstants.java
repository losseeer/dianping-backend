package com.hmdp.utils;

public class RedisConstants {
    public static final String LOGIN_CODE_KEY = "login:code:";
    public static final Long LOGIN_CODE_TTL = 2L;
    public static final String LOGIN_USER_KEY = "login:token:";
    public static final Long LOGIN_USER_TTL = 36000L;

    public static final Long CACHE_NULL_TTL = 2L;

    public static final Long CACHE_SHOP_TTL = 30L;
    public static final String CACHE_SHOP_KEY = "cache:shop:";

    public static final String LOCK_SHOP_KEY = "lock:shop:";
    public static final Long LOCK_SHOP_TTL = 10L;

    public static final String SECKILL_STOCK_KEY = "seckill:stock:";
    /**
     * 秒杀订单一人一单记录key前缀 —— Set结构，存储已下单的userId
     * 完整key: seckill:order:{voucherId}
     */
    public static final String SECKILL_ORDER_KEY = "seckill:order:";
    public static final String SECKILL_ORDER_STREAM_KEY = "stream.orders";
    public static final String SECKILL_ORDER_STREAM_GROUP = "g1";

    /**
     * 秒杀预订单（未完成落库的订单）缓存前缀 —— String，value=VoucherOrder JSON
     * 作用：秒杀返回后MQ异步写DB的窗口期，支付/取消/查详情/订单列表都能从此处捞到订单，
     *      避免前端立刻点"立即支付/取消订单"时出现"订单不存在"。
     * 完整key: seckill:order:pending:{orderId}
     */
    public static final String SECKILL_PENDING_ORDER_KEY = "seckill:order:pending:";
    public static final Long SECKILL_PENDING_ORDER_TTL = 10L;

    /**
     * 用户维度的未完成预订单索引 —— ZSet，value=orderId，score=createTime秒级时间戳
     * 作用："我的订单"列表先查DB，再补此索引里的 pending 订单，避免秒杀后立刻跳订单页看不到。
     * 完整key: seckill:order:pending:user:{userId}
     */
    public static final String SECKILL_PENDING_USER_KEY = "seckill:order:pending:user:";
    public static final Long SECKILL_PENDING_USER_TTL = 10L;

    /**
     * 全局预扣索引 —— ZSet，member = {orderId}:{userId}:{voucherId}:{streamEntryId}，
     * score = 下单 epoch 秒，由 seckill.lua 在预扣的同一次原子提交里写入。
     *
     * <p>
     * 【它解决的是"前两道防线都失效"的那一段】预扣能不能走到落库，前面有三层自愈
     * （PENDING 重读 / XCLAIM 认领 / 消费端幂等），但对账要回答的是另一个问题：
     * <strong>"Redis 究竟有没有一笔扣了库存、却永远不会有订单的残留"</strong>。
     * 这个问题只能靠一个跨订单维度的索引来扫，而 pending 预订单是按订单分片的、
     * 且只有10分钟TTL，过期就再也找不回来了。详见 {@code SeckillReservationReconciler}。
     *
     * <p>
     * 【为什么member是四段拼接】见 seckill.lua 3.9 的注释：一个 ZSet 兼任排序与载荷。
     */
    public static final String SECKILL_PENDING_INDEX_KEY = "seckill:order:pending:index";

    /**
     * 全局预扣索引的 TTL（秒）。
     *
     * 【它为什么必须远大于对账窗口】索引里每一条都自带删除时机——被对账任务确认之后 ZREM。
     * 这个 TTL 只是"对账任务长期停摆"时的内存兜底，绝不能小于对账窗口：
     * 否则一条本该被回滚的残留会在等到对账扫到它之前，先跟着整个键一起过期消失。
     * 当前 2 小时 vs 对账窗口 15 分钟，留了 8 倍余量。
     */
    public static final Long SECKILL_PENDING_INDEX_TTL = 2 * 60 * 60L;
    public static final String BLOG_LIKED_KEY = "blog:liked:";
    public static final String FEED_KEY = "feed:";
    public static final String SHOP_GEO_KEY = "shop:geo:";
    public static final String USER_SIGN_KEY = "sign:";

    public static final String SHOP_TYPE_KEY = "shop_type:";
    public static final Long SHOP_TYPE_LONG=10L;

    // ========== 搜索与推荐模块新增常量 ==========

    /**
     * 用户点赞的商铺集合
     * 结构：Set，value=shopId
     * 用于协同过滤：快速获取某用户点过赞的所有商铺
     */
    public static final String USER_LIKED_SHOPS_KEY = "user:liked:shops:";

    /**
     * 商铺的点赞用户集合
     * 结构：Set，value=userId
     * 用于协同过滤：快速找到点赞过同一商铺的其他用户
     */
    public static final String SHOP_LIKED_USERS_KEY = "shop:liked:users:";

    /**
     * 热门商铺排行
     * 结构：ZSet，value=shopId，score=sold(销量)
     * 用于全站热门推荐
     */
    public static final String SHOP_HOT_KEY = "shop:hot:";

    // ========== 短信验证码限流常量 ==========

    /**
     * 冷却键前缀 —— String，value="1"
     * 完整key: ratelimit:sms:cooldown:{phone}
     * 用 SET NX EX 占位，存在即表示冷却中。TTL 即冷却秒数。
     */
    public static final String SMS_LIMIT_COOLDOWN_KEY = "ratelimit:sms:cooldown:";

    /**
     * 日计数键前缀 —— String，value=当日成功发送次数
     * 完整key: ratelimit:sms:daily:{phone}
     * 首次自增时设置过期时间，TTL 即日窗口秒数（滚动窗口，非自然日）。
     */
    public static final String SMS_LIMIT_DAILY_KEY = "ratelimit:sms:daily:";

    /**
     * 全局计数键 —— String，value=全局窗口内成功发送次数
     * 完整key: ratelimit:sms:global
     * 挡手机号枚举攻击：攻击者用大量不同号码轰炸时，按号码限流是挡不住的。
     */
    public static final String SMS_LIMIT_GLOBAL_KEY = "ratelimit:sms:global";

    // ========== 接口限流常量 ==========

    /**
     * 接口令牌桶前缀 —— Hash { tokens, ts }，由 rate-limit.lua 读写
     * 完整key: ratelimit:api:{全限定类名}.{方法名}
     * 例：ratelimit:api:com.hmdp.controller.VoucherOrderController.seckillVoucher
     *
     * 为什么用全限定类名：这是个跨实例共享的命名空间，简单类名会碰撞
     * （将来加一个 admin.UserController 就会和现有的静默共用一个桶）。
     * TTL 由脚本固定为 2 秒，见 rate-limit.lua 末尾的推导。
     */
    public static final String RATE_LIMIT_API_KEY = "ratelimit:api:";

    // ========== 运行期动态配置（改完立刻生效，不重启） ==========

    /**
     * 动态配置命名空间前缀 —— String，value 是纯数字文本
     *
     * <p>
     * 【为什么单独开一个前缀而不是塞进各自的业务键】
     * 这一族键的唯一作用是「覆盖代码里的默认值」，读法、写法、校验、可观测口径都一致，
     * 放在一起才能用一个 {@code GET /config} 看全「当前哪些默认值被改了什么」。
     *
     * <p>
     * 【铁律：只存「运行期取来的值」，绝不存「已写进别处的值的副本」】
     * 反例就是把 qps 塞进令牌桶 Hash —— 改配置时旧键里的旧值会静默覆盖新配置，
     * 而且 TTL 一到规则跟着桶一起消失。这里的键只被读取、不被任何写入方回填，
     * 所以它永远是唯一事实源；删掉它 = 回到注解默认值，而不是回到某个陈旧副本。
     */
    public static final String DYNAMIC_CONFIG_KEY = "config:";

    /**
     * 接口限流阈值的运行期覆盖 —— String，value = qps（double，必须 &gt;= 1）
     * 完整key: config:ratelimit:rule:{全限定类名.方法名}
     *
     * 由 rate-limit.lua 作为 KEYS[2] 直接读取（不在 Java 侧读：热路径不许多一次 RTT，
     * 而且规则和桶必须在同一个原子脚本里生效）。注解上的 qps 退化为「没配规则时的默认值」。
     */
    public static final String RATE_LIMIT_RULE_KEY = DYNAMIC_CONFIG_KEY + "ratelimit:rule:";

    /**
     * Outbox 发布器的扫描间隔（毫秒）—— String，value = 正整数，取值范围见 DynamicConfig
     * 完整key: config:outbox:publish-interval-ms
     *
     * 为什么需要它：积压 5 万条时想立刻把间隔调到 200ms 追平，用注解值就得改配置重启，
     * 而重启本身又要重新预热缓存 —— 为一个开关付一次全量重启不值。
     */
    public static final String OUTBOX_PUBLISH_INTERVAL_KEY = DYNAMIC_CONFIG_KEY + "outbox:publish-interval-ms";
}
