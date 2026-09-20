-- =====================================================================
-- 接口限流脚本 —— Redis 分布式令牌桶
--
-- 【KEYS 契约】
--   KEYS[1] = ratelimit:api:{全限定类名.方法名}   Hash { tokens, ts }   桶状态
--   KEYS[2] = config:ratelimit:rule:{全限定类名.方法名}   String   运行期覆盖的 qps
--           不存在 → 完全按注解默认值走，行为与引入本键之前逐字节一致
--
-- 【ARGV 契约】（必须都是字符串）
--   ARGV[1] = capacity  桶容量（= 注解 qps × 1秒）
--   ARGV[2] = rate      每秒补充令牌数（= 注解 qps）
--
-- 【八股：返回值约定】
--   1 = 放行（拿到令牌）
--   0 = 拒绝（桶空）
--
-- =====================================================================
-- 【八股：为什么令牌桶的状态必须放 Redis？】
-- Guava 的 RateLimiter 是 JVM 进程内对象。部署 N 个实例，每个实例各有一个桶，
-- 配置的 qps 实际生效值是 N × qps —— 阈值成了摆设。而且实例重启桶就满了，
-- 相当于每次发布都送一波突发流量。放 Redis 后所有实例共用一个桶，阈值才是阈值。
--
-- 【八股：为什么 check-and-set 必须在一个 Lua 脚本里？】
-- "读 tokens/ts → 算补充量 → 判断够不够 → 扣减 → 写回"是五步。
-- 分五次调用 Redis，两个并发请求就可能读到同一个起点、各自扣减、双双放行。
-- Lua 在 Redis 里串行执行，把这五步焊成一个原子整体。
--
-- 【八股：为什么用 Redis 的时钟而不是 Java 传进来？】
-- 传客户端时间会把各实例的时钟偏差引入同一个共享桶：快的那台会算出巨大的
-- 补充量（凭空变出令牌），慢的那台算出的 elapsed 为负。Redis 的时钟是这套
-- 分布式状态里唯一的单一时间源。
--
-- =====================================================================
-- 【实现要点，改动前必读】
--
-- 1. 毫秒精度是必需的，不是优化
--    search 接口 qps=100，意味着每 10ms 补一个令牌。若时间戳只精确到秒，
--    补充量会每秒成批结算一次 —— 变成一个「每秒放行 100 个然后卡死」的
--    更漏的限流器。而且粗粒度测试照样能通过，不会有人发现。
--
-- 2. 不要把数值 tostring() 后再写回
--    Redis 把 Lua number 作为命令参数时会用 %.17g（精确往返），而 Lua 的
--    tostring 用 %.14g，对毫秒级时间戳（13 位整数）会丢失精度。
--    直接把 number 传进去。
--
-- 3. HMGET 对不存在的字段返回 false，不是 nil
--    所以判空必须走 tonumber()，写成 state[1] == nil 永远不会成立，
--    接着就会拿 false 做算术 —— 脚本报错 → 被上层当成「Redis 挂了」放行。
--    第 0 步读规则键同理：GET 不存在的键返回的是 false。
--
-- 4. 补充后必须 math.min(CAP, ...) 夹一次上限
--    漏掉的话，一个空闲了 1 小时的桶能瞬间放行 elapsed × qps 个请求，
--    等于没有限流；而且接口流量一降下来就会静默触发。
--
-- 5. 拒绝路径也写回状态
--    Guava 的 tryAcquire 在拒绝时是无副作用的；这里写回是为了顺带刷新 TTL。
--    两者效果相同（被拒时没消耗令牌，桶的存量不变），但机制不同，
--    不写清楚会被 review 当成 bug。
--
-- 6. 规则覆盖读在脚本里，不读在 Java 里
--    限流判定是热路径，每个请求先 GET 一次规则再 EVAL，等于凭空多一个 RTT，
--    而且「读到的规则」和「写回的桶」之间存在窗口：改规则的瞬间，
--    各实例可能各按新旧两套 CAP/RATE 补同一个桶。放进脚本就是原子的同一件事。
--    顺带把下面那条「滚动升级期间新旧实例共用桶会算歪」的局限也一起解决了 ——
--    阈值来源只有一个键，不存在两套。
-- =====================================================================

local CAP  = tonumber(ARGV[1])
local RATE = tonumber(ARGV[2])
local TTL  = 2   -- 秒。为什么是 2、为什么不做成 ARGV，见文件末尾的推导

-- 0. 运行期规则覆盖。BURST 从 ARGV 反推（capacity/rate 恒等于 Java 侧的
--    MAX_BURST_SECONDS），这样"突发窗口是几秒"这个决定只留在 Java 一处。
--    非数字或 < 1 的值一律忽略、退回注解值：qps < 1 会让桶容量不足 1，
--    等于静默打死这个接口（Java 侧对注解就是这么处理的）。写入接口有校验，
--    走到这个分支只可能是有人直接 redis-cli 塞了个非法值。
local BURST = CAP / RATE
local rule = redis.call('get', KEYS[2])
if rule then
    local override = tonumber(rule)
    if override and override >= 1 then
        RATE = override
        CAP = override * BURST
    end
end

-- 1. 取当前时间，毫秒精度
local t = redis.call('time')
local nowMs = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

-- 2. 读桶状态。冷桶（键不存在）视为满桶，与 Guava 的 maxPermits 初始状态一致
--    注意 CAP 已经是覆盖后的值：改规则后第一次请求就按新容量建冷桶
local state = redis.call('hmget', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local tsMs = tonumber(state[2])
if tokens == nil or tsMs == nil then
    tokens = CAP
    tsMs = nowMs
end

-- 3. 按流逝时间补充令牌
--    math.min 用的是新 CAP，所以把阈值从 500 调到 5 之后，桶里攒下的 500 个令牌
--    在这次调用里就被夹到 5 —— 不需要等旧令牌自然耗尽，改完立刻见效。
local elapsedMs = nowMs - tsMs
if elapsedMs < 0 then
    elapsedMs = 0        -- Redis 时钟回拨（如 NTP 校正）保护，否则会凭空扣令牌
end
tokens = math.min(CAP, tokens + elapsedMs * RATE / 1000)

-- 4. 取令牌
local allowed = 0
if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
end

-- 5. 写回并续期
redis.call('hset', KEYS[1], 'tokens', tokens, 'ts', nowMs)
redis.call('expire', KEYS[1], TTL)

return allowed

-- =====================================================================
-- 【TTL 为什么是 2 秒，以及为什么不能做成 ARGV】
--
-- 1) 为什么 TTL ≥ 1 秒时，过期在语义上是不可见的
--    桶空闲 T 秒后会补充 min(CAP, T × RATE) 个令牌。而 CAP/RATE = BURST = 1 秒
--    （引入规则覆盖后依然成立：覆盖时 RATE 和 CAP 是按同一个 BURST 一起改的），
--    所以只要 T ≥ 1，桶必然已经补满；此时键过期、下次重建的桶也是满的 ——
--    两种状态完全相同。唯一的偏差出现在 T < 1 的空闲间隙，过期会让限流器
--    略微更严，方向是安全的。
--    反过来，若 TTL < 1 秒，被持续压测的桶会周期性过期重置为满，
--    攻击者每 TTL 秒额外白拿 CAP 个请求 —— 平均值被抬高，且完全静默。
--
-- 2) 为什么不做成 ARGV
--    EXPIRE key 0 在 Redis 里是【删除键】。如果 TTL 由配置驱动，
--    配成 0 就会让桶每次都被清空 = 限流彻底失效，且不报任何错。
--    硬编码在脚本里，这个坑就不可能被踩到。
--
-- 【已知局限】
-- - 脚本非幂等：若 Redis 执行成功但应答丢失，重试会多扣一个令牌。
--   方向是「更严」而非「更松」，不会导致超发。
-- - 规则非法值是被忽略而不是被拒绝，且不留痕迹：写接口有校验，所以只有
--   直接 redis-cli 塞非法值才可能走到；此时生效的仍是注解值，
--   想确认当前真正生效的 qps 用 GET /config/ratelimit?api={全限定类名.方法名}
--   （它会同时回显注解默认值和键里的原始值）。
-- - 两个 KEYS 必须在同一个 Redis 实例上。单机/主从没问题；Redis Cluster 下
--   它们前缀不同会落到不同 slot，脚本会报 CROSSSLOT —— 届时的改法是给两个
--   键加同样的 hash tag：{ratelimit:api:xxx} 与 {ratelimit:api:xxx}:rule。
-- - 本脚本用了 redis.call('time')，需要 Redis ≥ 5（effects replication）。
--   Redis 7 已是默认行为，无需 redis.replicate_commands()。
-- =====================================================================
