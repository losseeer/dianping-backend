-- =====================================================================
-- 短信验证码限流脚本 —— 三道闸门，一次原子判定
--
-- 【KEYS 契约】
--   KEYS[1] = 冷却键   ratelimit:sms:cooldown:{phone}    String
--   KEYS[2] = 日计数键 ratelimit:sms:daily:{phone}       String（计数）
--   KEYS[3] = 全局键   ratelimit:sms:global              String（计数）
--
-- 【ARGV 契约】（必须全部是正整数字符串）
--   ARGV[1] = 冷却秒数      ARGV[2] = 日上限       ARGV[3] = 日窗口秒数
--   ARGV[4] = 全局上限      ARGV[5] = 全局窗口秒数
--
-- 【八股：返回值约定】
--   0 = 放行
--   1 = 冷却中（同一手机号发得太快）
--   2 = 超日上限（同一手机号当日发得太多）
--   3 = 超全局上限（全站发送量异常，疑似手机号枚举攻击）
--
-- 【八股：为什么这三道闸门必须放在同一个脚本里？】
-- "检查冷却键是否存在" 和 "写入冷却键" 是两步操作，分两次调用 Redis
-- 就是经典的 check-then-act 竞态：两个并发请求可能都查到"不存在"，
-- 然后都写入、都放行。Lua 脚本在 Redis 里串行执行，把这两步焊成一个整体。
-- 同理，"读日计数判断是否超限" 和 "自增日计数" 也必须是原子的。
--
-- 【八股：SET key val NX EX 为什么比 SETNX + EXPIRE 两步好？】
-- SETNX 和 EXPIRE 是两条命令，中间可能宕机/被抢占，导致键存在但没有过期时间
-- ——用户被永久锁死在冷却状态。SET ... NX EX 是单条命令，占位与设过期时间
-- 一次性完成，不存在这个窗口。这是"用一条原子命令消灭竞态"的典型例子。
--
-- 【设计：为什么要检查在前、计数在后？】
-- 三道检查全部不消耗任何状态，只有放行才累加计数。
-- 所以计数的语义是"成功发送次数"，而不是"请求次数"——被拒绝的请求
-- 不应该让用户当天的额度变少。
-- =====================================================================

-- 1. 全局上限检查（挡手机号枚举：攻击者用大量不同号码轰炸时，按号码限流完全挡不住）
local globalCount = tonumber(redis.call('get', KEYS[3])) or 0
if globalCount >= tonumber(ARGV[4]) then
    return 3
end

-- 2. 日上限检查（先于冷却，避免"已达上限"的用户白白烧掉一次冷却）
local dailyCount = tonumber(redis.call('get', KEYS[2])) or 0
if dailyCount >= tonumber(ARGV[2]) then
    return 2
end

-- 3. 冷却检查：SET NX 成功返回状态回复表（Lua 中为真值），键已存在返回 false
if not redis.call('set', KEYS[1], '1', 'NX', 'EX', ARGV[1]) then
    return 1
end

-- 4. 走到这里才累加计数。首次自增时设置过期时间，形成滚动窗口
local newDaily = redis.call('incr', KEYS[2])
if newDaily == 1 then
    redis.call('expire', KEYS[2], ARGV[3])
end

local newGlobal = redis.call('incr', KEYS[3])
if newGlobal == 1 then
    redis.call('expire', KEYS[3], ARGV[5])
end

return 0

-- =====================================================================
-- 【已知局限】（刻意不解决，写在这里避免后人踩坑）
--
-- 1. 滚动窗口 ≠ 严格滑动窗口
--    过期时间只在首次自增时设置，所以窗口是"从首次发送起算的 N 秒"。
--    跨窗口边界时，24h+1 分钟内可能发出 21 条。要严格限制"任意 24h 内
--    不超过 10 条"，需要用 ZSet 存发送时间戳，脚本里做
--    ZREMRANGEBYSCORE 清旧 + ZCARD 计数。成本更高，对这个场景不值。
--
-- 2. 应答丢失时计数已被消耗
--    如果 Redis 执行成功但应答在网络上丢了，Java 侧会走 fail-open 放行，
--    但冷却键和两个计数已经消耗掉了。表现为：用户拿到了验证码，下一次
--    却被告知"发送过于频繁"。这是 fail-open + 预先占位 的固有代价。
--
-- 3. 若迁移到 Redis Cluster，KEYS[1] 与 KEYS[2] 必须共享 hash tag
--    否则跨 slot 会报 CROSSSLOT。当前部署是单节点，故未加 tag。
-- =====================================================================
