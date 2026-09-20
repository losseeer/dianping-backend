# 秒杀压测 · JMeter

压 `POST /voucher-order/seckill/105`：1000 并发抢 500 库存，压完四维度对账验证零超卖。

| 文件 | 作用 |
|---|---|
| `prepare.sh` | 重置库存与活动窗口 + 注入 1000 个会话。**每轮压测前必跑** |
| `seckill.jmx` | JMeter 脚本（1000 线程、Ramp-up=0、集合点 groupSize=1000） |
| `jmeter.properties` | 压测端连接池调优，命令行 `-q` 加载 |
| `verify.sh` | 压测后四维度对账 |
| `tokens.csv` | 1000 行 `token,userId`，由 `prepare.sh` 生成 |
| `result.jtl`、`report/` | 运行产物 |

---

## 一、跑一轮

```bash
# 终端 1 —— 起应用（JDK 8）→ :8081
#   需要 MySQL / Redis / RabbitMQ；ES 不用起（自动降级 MySQL LIKE）
mvn spring-boot:run
```

```bash
# 终端 2 —— 从仓库根目录执行
cd stress
./prepare.sh 105 1000 500 --reset-orders

JM=~/Downloads/apache-jmeter-5.6.3        # 改成你的 JMeter 解压目录
rm -rf report result.jtl
JMETER_COMPLETE_ARGS=true JVM_ARGS="-Xms2g -Xmx4g -server" \
  $JM/bin/jmeter -n -q jmeter.properties -t seckill.jmx -l result.jtl -e -o ./report

open report/index.html
./verify.sh 105 500
```

`--reset-orders` 做两件事（破坏性，所以要显式加）：

1. **删除该券历史订单** —— 不加的话对账必然 FAIL：订单是累加的，而「库存守恒」是 `DB剩余库存 + 有效订单数 == 初始库存`。残留订单还会占着 `uk_active_user_voucher` 唯一索引，造成 Redis 放行、DB 拒绝。
2. **清空 `order.delay.queue` / `order.cancel.queue`** —— 延迟消息是**落库成功后**才发的，入队时订单确实存在；删订单**删不掉已经躺在 MQ 里的消息**。不清的话，它们照常 30 分钟后到期，回调找不到订单，于是持续半小时刷：

```
WARN 超时取消失败，订单不存在: orderId=638562427465641830
```

> **看到这条 WARN 怎么办**：不用管，处理是幂等且无副作用的——代码里有 pending 缓存兜底，但那个 TTL 只有 10 分钟，够不着 30 分钟后才到的消息，于是走到 `log.warn` 就返回。
> 只有在队列**没被清掉**时才会出现：RabbitMQ 未启动、`rabbitmqctl` 缺失（脚本会告警并跳过），或你手工删了订单却没清队列。

跑多轮就重复「prepare → jmeter → verify」三步。

---

## 二、四种失败：怎么认、怎么修

跑完先看 summary 那一行。**判据是样本数必须是 1000，`Err` 接近 0**——不是只看 `Err`。

| 什么时候出问题 | 症状 / 怎么认 | 修 |
|---|---|---|
| **JMeter 根本没启动** | 报错后直接退出，**不打印 `summary` 行**。三种报错之一：<br>`Error: VM option 'UseG1GC' is experimental...`<br>`Cannot write to '.../report' as folder is not empty`<br>`Error in NonGUIDriver ... Results file:result.jtl is not empty` | 依次检查：命令前有 `JMETER_COMPLETE_ARGS=true`；已 `rm -rf report result.jtl` |
| **启动了，但一个请求都没发** | ⚠️ **正常打印 `end of run`，summary 是 `0 in 00:00:00`，`Err: 0 (0.00%)` —— 看着像成功。** 真错误只在 `jmeter.log` 里：`File tokens.csv must exist and be readable` | 先 `cd stress` 再启动（相对路径按**启动目录**解析，不是 JMX 所在目录） |
| **跑完了，但对账 FAIL** | JMeter 这边一切正常，`verify.sh` 却说库存/订单对不上 | 应用侧 `spring.redis.timeout` 必须 ≥ 3000ms |

**「一个请求都没发」这条最坑**：`Err: 0 (0.00%)` 配上 `0 in 00:00:00` 是**绿色假象**——零样本自然零错误。真错误写进 `jmeter.log` 而不进 stdout，`grep Err` 完全看不出来。**判据是样本数，不是错误数：每次都要确认 summary 里是 `1000 in`。**

**「对账 FAIL」这条的原理**：`spring.redis.timeout` 设短了会掐断 `SeckillVoucherListener` 的 `XREADGROUP ... BLOCK 2秒`，秒杀订单**全部写不进 DB**，而单测和接口冒烟全绿——只有压测能发现。

### 改并发数

**`-JTHREADS=2000` 不生效**——`-J` 设的是 JMeter property，而 `${THREADS}` 读的是 jmx 里 Test Plan 的 User Defined Variable，后者优先。直接改 jmx 里这两处，**必须同时改**：

| 位置 | 值 |
|---|---|
| Test Plan → User Defined Variables → `THREADS` | 1000 |
| 集合点 → `SyncTimer.groupSize` | 1000 |

> 两者不等**不会挂起**（实测 10 线程配 groupSize 1000 可正常跑完），但集合点会失效，测到的就不是瞬时并发了。

---

## 三、其他场景

**场景 B：关限流测后端容量**（给第四节调阈值用）

阈值不用改代码重启了，运行期覆盖就行（详见 `docs/backend-design.md` §5.4）。
`$TOKEN` 的取法见下面场景 C；它是**登录用户**的 token，`/config/**` 不在登录白名单里。

```bash
API=com.hmdp.controller.VoucherOrderController.seckillVoucher

# 1. 把秒杀阈值顶到写入接口允许的上限（100000），全部实例下一个请求起生效
curl -s -X PUT -H "authorization: $TOKEN" "http://127.0.0.1:8081/config/ratelimit?api=$API&qps=100000"

# 2. 重跑「一、跑一轮」那三步（prepare → jmeter → verify）

# 3. 删掉覆盖，回到注解上的 qps=50
curl -s -X DELETE -H "authorization: $TOKEN" "http://127.0.0.1:8081/config/ratelimit?api=$API"
```

> **还原这件事现在可以自查**：改完立刻 `curl -s -H "authorization: $TOKEN" http://127.0.0.1:8081/config`，
> 那一行的 `overridden` 还是 `true` 就是没还原。以前"忘了还原 = 秒杀完全不设防"只能靠记性，
> 现在是一条命令看出来 —— 但也因此多了一个以前没有的失效模式：**覆盖只活在 Redis 里**，
> `FLUSHALL` 或换实例就悄悄回到 50；所以压测报告的复现步骤里必须写清当时那一行的 effectiveQps。
>
> 还有：`prepare.sh` 每轮清的是 `ratelimit:api:*` 令牌桶，**不清** `config:ratelimit:rule:*` 覆盖。
> 所以带着覆盖连跑两轮，第二轮的阈值不会自己恢复原状。

**场景 C：ab 交叉验证**（排除 JMeter 自身瓶颈）

```bash
P=13800138000
redis-cli DEL "ratelimit:sms:cooldown:$P" "ratelimit:sms:daily:$P"
curl -s -o /dev/null -X POST "http://127.0.0.1:8081/user/code?phone=$P"
CODE=$(redis-cli GET "login:code:$P")
TOKEN=$(curl -s -X POST http://127.0.0.1:8081/user/login -H 'Content-Type: application/json' \
        -d "{\"phone\":\"$P\",\"code\":\"$CODE\"}" | sed -n 's/.*"data":"\([^"]*\)".*/\1/p')

: > /tmp/empty_post.txt
ab -n 3000 -c 1000 -k -p /tmp/empty_post.txt -T "application/json" \
   -H "authorization: $TOKEN" http://127.0.0.1:8081/voucher-order/seckill/105
```

> ab 全程复用同一个 token，最多创建 1 单，其余全在拒绝路径。它衡量的是**拒绝路径**吞吐，不是下单容量。

---

## 四、审阅结果

**先看对账，再看吞吐。** 四项全 PASS 才算有效：

| 判定 | 等式 |
|---|---|
| 库存守恒 | DB 剩余库存 + 成交订单数 == 初始库存 |
| 一人一单 | 去重买家数 == 订单数 |
| 缓存一致 | Redis 库存 == DB 库存 |
| Redis Set | Redis 订单 Set 基数 == DB 订单数 |

```bash
./verify.sh 105 500
```

对账 FAIL 按序排查：

1. `DB剩余库存 + 成交订单数 > 初始库存` → 历史订单没清，加 `--reset-orders` 重跑
2. `Redis 订单数 ≠ DB 订单数` → 同上，残留订单占了唯一索引
3. **两边都是 0** → 应用侧 Redis 超时把 Stream 消费端打断了（见第二节末条）
4. `redis-cli XPENDING stream.orders seckill-group` → 看有无堆积消息

> 手工删 `stream.orders` **必须先停应用**。消费组只在启动时创建，运行中删掉会持续报
> `NOGROUP No such key 'stream.orders' or consumer group 'g1'`，异步落库全程失效。
> 顺序：停应用 → 清 Redis → 启应用。

**再看吞吐**（`report/index.html`）：先确认 `Err` 接近 0，再看 Throughput / P99。断言把 `200` 和 `429` 都算通过——429 是限流拒绝，属预期；只有连接错误和 5xx 是真失败。

两个误报：

- **第一轮低 30% 是冷启动**（JIT + 连接池预热）。实测 4643 → 6608 / 8059 / 7158 QPS。**至少跑 3 轮**，别拿单次采样下结论。
- **ab 的 `Failed requests` 常是假的**：明细为 `Connect: 0, Receive: 0, Length: N, Exceptions: 0` 即只是响应体长度差异（成功/售罄的 JSON 长度不同）。只有 Connect / Receive / Exceptions 非 0 才是真错误。

历史基线（完整数据见 [`docs/perf-report.md`](../docs/perf-report.md)）：

| 场景 | 结果 |
|---|---|
| 场景 A（Guava 限流，2026-08-20） | 110 单 / 2.38s |
| 场景 A′（Redis 限流，2026-09-17） | 64 单 / 1.0s |
| ab 拒绝路径吞吐 | 6579 → 6608 / 8059 / 7158 QPS |

> 下单数都符合 `容量 + 速率 × 令牌消耗窗口`：`50+50×1.3≈110`、`50+50×0.28≈64`。数对不上时先套这个公式，能立刻分清是限流器变了还是压测端变了。

---

## 五、用结果调阈值

```
新阈值 = 0.6 ~ 0.8 × 场景B测出的容量C
改完必须重跑场景 A，确认对账全 PASS 且 P99 可接受
```

| 接口 | qps | failOpen | 能不能调 |
|---|---|---|---|
| 秒杀 `seckillVoucher` | 50 | false | ❌ **不要动**：这是业务漏斗宽度不是容量上限。1000 人抢 500 件，一半注定失败，让失败请求在入口毫秒级返回才是它的价值；调低不伤业务，调高才伤 |
| 搜索 `search` | 100 | true | ❌ **本机测不了**：为保护 ES 而设，但本机 ES 未起，测到的是 MySQL LIKE 降级路径容量，**偏大**——拿它定阈值会设出 ES 扛不住的值 |
| 支付 `pay` | 20 | false | ⚠️ 需自建脚本（脚手架未覆盖）；瓶颈通常在外部渠道不在本服务 |
| 登录 `login` | 5 | true | ❌ **不是数值问题**：全局 5 QPS = 5 个攻击者就能让全站登不上，该改成按手机号/IP 维度（参照 `SmsRateLimiter`） |
| 发验证码 `sendCode` | — | — | 已按手机号限流，不适用本表 |

> 表里的 `qps` 是**注解默认值**。场景 B 那种运行期覆盖不会改这张表，也不会跟着代码走 ——
> 覆盖只存在于 Redis 的 `config:ratelimit:rule:{api}` 键里，**只有 `DELETE /config/ratelimit?api=…`
> 才回到本表的数字**；重启应用不吃掉它（覆盖在 Redis 里，不在 JVM 里）。调完阈值请重跑场景 A 对账。

**三个陷阱**：

1. **localhost 单机 ≠ 生产容量。** 压测端与服务端抢同一台 CPU，Redis/MySQL 也是本机进程。数字只能做改前改后的**相对对比**。
2. **别拿拒绝路径的吞吐定阈值。** ab 那 6579 QPS 是 3000 请求打向 500 库存的券，**至少 83% 走拒绝路径**（Lua 预检就返回）。下单路径应看 B3：500 单 / 1.107s ≈ **452 单/秒**。差一个数量级。
3. **阈值改完要重跑场景 A 验证。** 容量是「后端能扛多少」，阈值是「允许多少进来」，两者之间还隔着限流器本身的正确性（令牌桶容量、突发窗口）。
