#!/bin/bash
# ==============================================================================
# 秒杀压测 · 四维度对账
#
# 用法：./verify.sh [voucherId] [期望库存=500]
#
# 【为什么必须对账】
# QPS/延迟只证明"快"，对账才证明"没错"。零超卖不能靠口号，要用数据证明：
#   1. DB 库存 + 成交订单数 == 初始库存   （不多不少）
#   2. DB 买家去重数 == 成交订单数        （一人一单，无重复下单）
#   3. Redis 库存 == DB 库存              （缓存与 DB 一致）
#   4. Redis 订单 Set 基数 == DB 订单数   （Redis 层一人一单记录一致）
#
# 【收敛等待】异步落库（Redis Stream 消费）有延迟，必须等订单数稳定再对账，
#   否则会读到中间态。策略：连续 3 次采样（间隔 2 秒）不变即判定收敛。
# ==============================================================================
set -u

VOUCHER_ID=${1:-105}
EXPECTED_STOCK=${2:-500}
DB="dingping"
MYSQL="mysql -uroot ${DB} -N"

echo "=== 等待异步落库收敛（连续 3 次采样不变）==="
LAST=-1
STABLE=0
ATTEMPT=0
while [ ${STABLE} -lt 3 ] && [ ${ATTEMPT} -lt 30 ]; do
  CUR=$(${MYSQL} -e "SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id=${VOUCHER_ID} AND status NOT IN (4,5);" 2>/dev/null | tr -d ' ')
  if [ "${CUR}" = "${LAST}" ]; then
    STABLE=$((STABLE+1))
  else
    STABLE=0
  fi
  echo "  第 ${ATTEMPT} 次采样：有效订单 ${CUR} (稳定计数 ${STABLE}/3)"
  LAST=${CUR}
  ATTEMPT=$((ATTEMPT+1))
  [ ${STABLE} -lt 3 ] && sleep 2
done

ORDERS=${LAST}
BUYERS=$(${MYSQL} -e "SELECT COUNT(DISTINCT user_id) FROM tb_voucher_order WHERE voucher_id=${VOUCHER_ID} AND status NOT IN (4,5);" | tr -d ' ')
DB_STOCK=$(${MYSQL} -e "SELECT stock FROM tb_seckill_voucher WHERE voucher_id=${VOUCHER_ID};" | tr -d ' ')
RD_STOCK=$(redis-cli GET "seckill:stock:${VOUCHER_ID}" 2>/dev/null | tr -d ' ')
RD_SET=$(redis-cli SCARD "seckill:order:${VOUCHER_ID}" 2>/dev/null | tr -d ' ')

echo ""
echo "================= 对账结果 ================="
printf "DB   成交订单数        : %s\n" "${ORDERS}"
printf "DB   去重买家数        : %s\n" "${BUYERS}"
printf "DB   剩余库存          : %s\n" "${DB_STOCK}"
printf "Redis 剩余库存         : %s\n" "${RD_STOCK:-nil}"
printf "Redis 订单Set基数      : %s\n" "${RD_SET:-nil}"
echo "--------------------------------------------"

PASS=0
FAIL=0
check() {
  if [ "$2" = "$3" ]; then
    printf "  [PASS] %-28s %s == %s\n" "$1" "$2" "$3"
    PASS=$((PASS+1))
  else
    printf "  [FAIL] %-28s %s != %s\n" "$1" "$2" "$3"
    FAIL=$((FAIL+1))
  fi
}

SUM=$((DB_STOCK + ORDERS))
check "库存守恒(DB库存+订单=初始)" "${SUM}" "${EXPECTED_STOCK}"
check "一人一单(买家数=订单数)"    "${BUYERS}" "${ORDERS}"
check "缓存一致(Redis库存=DB库存)" "${RD_STOCK:-nil}" "${DB_STOCK}"
check "一人一单(RedisSet=DB订单)"  "${RD_SET:-nil}" "${ORDERS}"

echo "--------------------------------------------"
echo "PASS ${PASS} / FAIL ${FAIL}"
if [ ${FAIL} -eq 0 ]; then
  echo "结论：零超卖、零重复下单，Redis 与 DB 完全一致 ✅"
else
  echo "结论：存在不一致 ❌ —— 检查 Stream 消费端日志与 pending 消息"
  echo "  redis-cli XPENDING stream.orders seckill-group"
fi
