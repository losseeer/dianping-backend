#!/bin/bash
# ==============================================================================
# 秒杀压测 · 数据准备脚本
#
# 作用：把 DB / Redis 恢复到"可复现"的初始状态，并注入 N 个登录会话 token
#
# 用法：
#   ./prepare.sh                                # 默认券 105、1000 用户、库存 500
#   ./prepare.sh 105 1000 500                   # 手动指定
#   ./prepare.sh 105 1000 500 --reset-orders    # 额外清空该券历史订单（破坏性）
#
# 【为什么需要这个脚本】
# 1. 秒杀接口会校验活动窗口（begin_time <= now < end_time），券 105 的
#    end_time 是 2026-08-20，已过期，不刷新会全部返回"秒杀已经结束"
# 2. 登录接口带 @RateLimit(qps=5)，压测绝不能走它 —— 直接写 Redis 注入会话
# 3. 每次压测前重置库存/订单 Set，保证多轮结果可对比
# 4. 默认【不删订单】，所以重复压测会累积。对账的「库存守恒」是
#    DB剩余库存 + 该券有效订单数 == 初始库存，历史订单会让它永远不成立；
#    旧订单还占着 uk_active_user_voucher 索引，导致新轮次里 Redis 放行、
#    DB 拒绝。要干净结果就加 --reset-orders。
#
# 默认非破坏性：只改 tb_seckill_voucher 的 stock/时间窗 + 删 Redis 秒杀键
#              （缓存会被 ensureRedisStock 自动重建），不删任何订单数据
# ==============================================================================
set -e

# 可选破坏性开关：--reset-orders 会【删除】该券的历史订单
RESET_ORDERS=0
POSITIONAL=()
for arg in "$@"; do
  case "${arg}" in
    --reset-orders) RESET_ORDERS=1 ;;
    *) POSITIONAL+=("${arg}") ;;
  esac
done

VOUCHER_ID=${POSITIONAL[0]:-105}
USER_COUNT=${POSITIONAL[1]:-1000}
STOCK=${POSITIONAL[2]:-500}
DB="dingping"
MYSQL="mysql -uroot ${DB}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TOKENS_CSV="${SCRIPT_DIR}/tokens.csv"

echo "=== [0/5] 参数 ==="
echo "voucher_id=${VOUCHER_ID}  users=${USER_COUNT}  stock=${STOCK}  reset_orders=${RESET_ORDERS}"

# ------------------------------------------------------------------------------
echo "=== [1/5] 刷新活动窗口与库存 ==="
# 【为什么订单表也要清】对账的「库存守恒」check 是
#      DB剩余库存 + 该券有效订单数 == 初始库存
# 而订单是【累加】的：prepare.sh 默认不删订单（非破坏性），所以重复压测时
# 上一轮的订单会让这个等式永远不成立，verify 直接 FAIL —— 而这与本次压测
# 无关。更隐蔽的是：旧订单还占着 uk_active_user_voucher 唯一索引，会让新一
# 轮里 Redis 放行、DB 却拒绝的用户出现，表现为「Redis 订单数 > DB 订单数」。
# 想复现干净结果就加 --reset-orders。
if [ "${RESET_ORDERS}" -eq 1 ]; then
  BEFORE=$(${MYSQL} -N -e "SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id=${VOUCHER_ID};" | tr -d ' ')
  ${MYSQL} -e "DELETE FROM tb_voucher_order WHERE voucher_id=${VOUCHER_ID};"
  echo "已删除该券历史订单 ${BEFORE} 条"
else
  LEFT=$(${MYSQL} -N -e "SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id=${VOUCHER_ID};" | tr -d ' ')
  if [ "${LEFT}" -gt 0 ]; then
    echo "WARN: 该券已有 ${LEFT} 条历史订单，对账的「库存守恒」将失败。"
    echo "      要干净结果请重跑：./prepare.sh ${VOUCHER_ID} ${USER_COUNT} ${STOCK} --reset-orders"
  fi
fi

${MYSQL} -e "
UPDATE tb_seckill_voucher
   SET stock      = ${STOCK},
       begin_time = NOW() - INTERVAL 1 HOUR,
       end_time   = NOW() + INTERVAL 1 DAY
 WHERE voucher_id = ${VOUCHER_ID};
"
${MYSQL} -e "SELECT voucher_id, stock, begin_time, end_time FROM tb_seckill_voucher WHERE voucher_id=${VOUCHER_ID};"

# ------------------------------------------------------------------------------
echo "=== [2/5] 从 DB 取 ${USER_COUNT} 个真实 userId ==="
# 必须取真实 userId：订单表有 user_id 外键语义，后续对账/订单列表才真实
${MYSQL} -N -e "SELECT id FROM tb_user ORDER BY id LIMIT ${USER_COUNT};" > /tmp/_user_ids.txt
ACTUAL=$(wc -l < /tmp/_user_ids.txt | tr -d ' ')
echo "取到 ${ACTUAL} 个 userId"
if [ "${ACTUAL}" -lt "${USER_COUNT}" ]; then
  echo "WARN: tb_user 只有 ${ACTUAL} 行，压测并发将按 ${ACTUAL} 生效"
fi

# ------------------------------------------------------------------------------
echo "=== [3/5] 生成 tokens.csv （JMeter CSV Data Set 用，无表头）==="
: > "${TOKENS_CSV}"
i=0
while read -r uid; do
  i=$((i+1))
  echo "tk${i},${uid}" >> "${TOKENS_CSV}"
done < /tmp/_user_ids.txt
echo "已生成 ${TOKENS_CSV} （${i} 行，格式：token,userId）"
head -3 "${TOKENS_CSV}"

# ------------------------------------------------------------------------------
echo "=== [4/5] 注入会话到 Redis + 清理秒杀缓存 ==="
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
# 清理上一轮残留的接口令牌桶
# 【为什么必须清】@RateLimit 已改为 Redis 分布式令牌桶（ratelimit:api:*）。
# 上一轮压测打空了的桶如果留到这一轮，会压制场景 A 的冷启动突发，
# "成功下单数"这个数字就会因为与本次改动无关的原因漂移，失去可比性。
redis-cli --scan --pattern "ratelimit:api:*" 2>/dev/null | xargs -r -n 100 redis-cli DEL > /dev/null 2>&1 || true

echo "验证会话：$(redis-cli HGETALL login:token:tk1)"
echo "当前 Redis 库存：$(redis-cli GET seckill:stock:${VOUCHER_ID})  (nil 表示尚未预热，首次请求自动创建)"

echo ""
echo "=== 准备完成 ==="
echo "接下来：JMeter GUI → File → Open → stress/seckill.jmx"
echo "   或命令行：cd stress && JVM_ARGS='-Xms2g -Xmx4g' jmeter -n -t seckill.jmx -l result.jtl -e -o ./report"
