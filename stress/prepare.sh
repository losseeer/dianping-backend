#!/bin/bash
# ==============================================================================
# 秒杀压测 · 数据准备脚本
#
# 作用：把 DB / Redis 恢复到"可复现"的初始状态，并注入 N 个登录会话 token
#
# 用法：
#   ./prepare.sh                     # 默认券 105、1000 用户、库存 500
#   ./prepare.sh 105 1000 500        # 手动指定
#
# 【为什么需要这个脚本】
# 1. 秒杀接口会校验活动窗口（begin_time <= now < end_time），券 105 的
#    end_time 是 2026-08-20，已过期，不刷新会全部返回"秒杀已经结束"
# 2. 登录接口带 @RateLimit(qps=5)，压测绝不能走它 —— 直接写 Redis 注入会话
# 3. 每次压测前重置库存/订单 Set，保证多轮结果可对比
#
# 非破坏性：只改 tb_seckill_voucher 的 stock/时间窗，删除 Redis 秒杀缓存键
#           （缓存可被 ensureRedisStock 自动重建），不删任何订单数据
# ==============================================================================
set -e

VOUCHER_ID=${1:-105}
USER_COUNT=${2:-1000}
STOCK=${3:-500}
DB="dingping"
MYSQL="mysql -uroot ${DB}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TOKENS_CSV="${SCRIPT_DIR}/tokens.csv"

echo "=== [0/4] 参数 ==="
echo "voucher_id=${VOUCHER_ID}  users=${USER_COUNT}  stock=${STOCK}"

# ------------------------------------------------------------------------------
echo "=== [1/4] 刷新活动窗口与库存 ==="
${MYSQL} -e "
UPDATE tb_seckill_voucher
   SET stock      = ${STOCK},
       begin_time = NOW() - INTERVAL 1 HOUR,
       end_time   = NOW() + INTERVAL 1 DAY
 WHERE voucher_id = ${VOUCHER_ID};
"
${MYSQL} -e "SELECT voucher_id, stock, begin_time, end_time FROM tb_seckill_voucher WHERE voucher_id=${VOUCHER_ID};"

# ------------------------------------------------------------------------------
echo "=== [2/4] 从 DB 取 ${USER_COUNT} 个真实 userId ==="
# 必须取真实 userId：订单表有 user_id 外键语义，后续对账/订单列表才真实
${MYSQL} -N -e "SELECT id FROM tb_user ORDER BY id LIMIT ${USER_COUNT};" > /tmp/_user_ids.txt
ACTUAL=$(wc -l < /tmp/_user_ids.txt | tr -d ' ')
echo "取到 ${ACTUAL} 个 userId"
if [ "${ACTUAL}" -lt "${USER_COUNT}" ]; then
  echo "WARN: tb_user 只有 ${ACTUAL} 行，压测并发将按 ${ACTUAL} 生效"
fi

# ------------------------------------------------------------------------------
echo "=== [3/4] 生成 tokens.csv （JMeter CSV Data Set 用，无表头）==="
: > "${TOKENS_CSV}"
i=0
while read -r uid; do
  i=$((i+1))
  echo "tk${i},${uid}" >> "${TOKENS_CSV}"
done < /tmp/_user_ids.txt
echo "已生成 ${TOKENS_CSV} （${i} 行，格式：token,userId）"
head -3 "${TOKENS_CSV}"

# ------------------------------------------------------------------------------
echo "=== [4/4] 注入会话到 Redis + 清理秒杀缓存 ==="
# 会话结构：key = login:token:{token}，Hash 字段 id / nickName / icon
# 对应 UserServiceImpl 登录时的写入格式，RefreshTokenInterceptor 直接读
awk -F',' '{printf "HSET login:token:%s id %s nickName u%s icon -\nEXPIRE login:token:%s 36000\n", $1, $2, $2, $1}' "${TOKENS_CSV}" \
  | redis-cli --pipe > /dev/null

# 清理上一轮残留的秒杀状态（ensureRedisStock 会在首次请求时重建库存）
redis-cli DEL "seckill:stock:${VOUCHER_ID}" > /dev/null
redis-cli DEL "seckill:order:${VOUCHER_ID}" > /dev/null
# 清理预订单缓存（key 形如 seckill:order:pending:*）
redis-cli --scan --pattern "seckill:order:pending:*" 2>/dev/null | xargs -r -n 100 redis-cli DEL > /dev/null 2>&1 || true
# 清理库存恢复幂等标记
redis-cli --scan --pattern "seckill:restored:*" 2>/dev/null | xargs -r -n 100 redis-cli DEL > /dev/null 2>&1 || true

echo "验证会话：$(redis-cli HGETALL login:token:tk1)"
echo "当前 Redis 库存：$(redis-cli GET seckill:stock:${VOUCHER_ID})  (nil 表示尚未预热，首次请求自动创建)"

echo ""
echo "=== 准备完成 ==="
echo "接下来：JMeter GUI → File → Open → stress/seckill.jmx"
echo "   或命令行：cd stress && JVM_ARGS='-Xms2g -Xmx4g' jmeter -n -t seckill.jmx -l result.jtl -e -o ./report"
