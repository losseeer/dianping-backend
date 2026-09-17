# 秒杀压测 · JMeter

对 `POST /voucher-order/seckill/{voucherId}`（券 105、1000 并发）压测，并用四维度对账验证零超卖。

| 文件 | 作用 |
|---|---|
| `prepare.sh` | 重置状态 + 注入 1000 个登录会话（生成 `tokens.csv`），**每轮压测前必跑** |
| `tokens.csv` | 1000 行 `token,userId`（无表头），JMeter 参数化用 |
| `seckill.jmx` | JMeter 脚本 |
| `jmeter.properties` | JMeter 属性覆盖层（连接池调优），由命令行 `-q` 加载 |
| `verify.sh` | 压测后四维度对账 |
| `result.jtl`、`report/` | 运行产物 |

---

# 一、压测设置

## 1.1 被测目标

| 项 | 值 |
|---|---|
| 接口 | `POST /voucher-order/seckill/105` |
| 并发 | 1000 线程，Ramp-up=0，Loop=1（**瞬时并发**，非渐进爬坡） |
| 数据 | 1000 个真实用户 / 初始库存 500 / 券 105 |
| 放行方式 | **集合点**（Synchronizing Timer，groupSize=1000）——1000 线程全部就绪才同一瞬间放行 |

## 1.2 限流配置现状

| 接口 | qps | failOpen | 这个值的性质 |
|---|---|---|---|
| 秒杀 `seckillVoucher` | 50 | `false` | **业务漏斗宽度**，不是容量上限（见第四节） |
| 搜索 `search` | 100 | `true` | 保护 ES 的容量 |
| 支付 `pay` | 20 | `false` | 保护支付链路 |
| 登录 `login` | 5 | `true` | 防撞库（**维度存疑**，见第四节） |
| 发验证码 `sendCode` | — | — | 已改为按手机号限流（`SmsRateLimiter`），不适用本表 |

- 秒杀 / 支付设 `failOpen=false`：丢钱路径，Redis 挂了宁可拒绝
- 搜索 / 登录用默认 `true`：丢防护，可用性优先

## 1.3 压测端调优（已随仓库分发，不用改 JMeter 安装目录）

| 项 | 值 | 为什么 |
|---|---|---|
| `httpclient4.max_per_route` | 1000（默认 **2**） | 默认值会让 1000 线程共享 2 条连接排队建连，测出的是建连开销而非服务端能力 |
| HTTP KeepAlive | 勾选 | 规避 macOS `somaxconn=128` 的 SYN 重传退避 |
| 连接预热 | 正式压测前打一轮小流量 | 消除 JIT 编译与连接池冷启动（**冷启动会低 30%**，见第三节） |

## 1.4 环境约束（不满足就跑不起来）

**1. `JMETER_COMPLETE_ARGS=true` 不能省。** JMeter 启动脚本无条件注入 `-XX:+UseG1GC`，本机 JDK `1.8.0_491` 判其为实验特性，直接退出：

```
Error: VM option 'UseG1GC' is experimental and must be enabled via -XX:+UnlockExperimentalVMOptions.
```

该变量会清空脚本自带的 ARGS，让 `JVM_ARGS` 完全接管。G1 在 JDK 8 上非必需。

**2. 必须先 `cd stress`。** 相对路径按 JMeter 的**启动目录**解析，不是 JMX 所在目录，否则报 `File tokens.csv must exist and be readable`。

**3. 两个输出产物都必须先删干净。**
- `-o ./report` 要求目录不存在或为空 → 否则报 `Cannot write to '.../report' as folder is not empty`
- `-l result.jtl` 同样要求文件不存在或为空 → 否则报 `Error in NonGUIDriver ... Results file:result.jtl is not empty`

所以每次都先 `rm -rf report result.jtl`。**两个报错都发生在压测开始之前**，JMeter 会直接退出且**不打印 summary 行**——如果你用 `grep` 过滤输出（比如只看 `summary`），很容易把"根本没跑"误认成"跑了没输出"。

**4. 改线程数必须同步改 `SyncTimer.groupSize`**（jmx 里写死 1000，未参数化）。两者不等会让线程永久阻塞在集合点。

**5. 应用侧：`spring.redis.timeout` 必须大于最长阻塞命令。** 这是**跑压测才会暴露**的坑——超时设短了，`SeckillVoucherListener` 的 `XREADGROUP ... BLOCK 2 秒` 每次都被掐断，表现是**秒杀订单全部写不进 DB、verify 直接 FAIL**，而单测和接口冒烟全绿。当前值 3000ms（2 秒阻塞 + 1 秒余量）。

---

# 二、压测步骤

## 场景 A：限流开启（生产配置，验证削峰）

```bash
cd stress            # 在仓库根目录执行

# 1. 起应用（另开终端，JDK 8）—— mvn spring-boot:run  → :8081
#    依赖 MySQL / Redis / RabbitMQ；ES 不用起（检索自动降级到 MySQL LIKE）

# 2. 准备：券 105、1000 用户、库存 500
./prepare.sh 105 1000 500

# 3. 压测
JM=~/Downloads/apache-jmeter-5.6.3                        # 改成你的 JMeter 解压目录
rm -rf report result.jtl                                  # 两个产物都必须先删，见 1.4
JMETER_COMPLETE_ARGS=true JVM_ARGS="-Xms2g -Xmx4g -server" \
  $JM/bin/jmeter -n -q jmeter.properties -t seckill.jmx -l result.jtl -e -o ./report
open report/index.html

# 4. 对账（压测后必跑）
./verify.sh 105 500
```

`prepare.sh` 做的三件事：刷新活动窗口（券 105 原 `end_time` 已过期，不改会全部返回"秒杀已经结束"）、从 `tb_user` 取 1000 个**真实** userId 直写 Redis 会话（登录接口有 `@RateLimit(qps=5)`，压测不能走它）、清理上轮 Redis 残留键。

> 清理项含 `ratelimit:api:*`——`@RateLimit` 已是 Redis 分布式令牌桶，上一轮打空的桶若留到这一轮会压制冷启动突发，"成功下单数"就会因与改动无关的原因漂移。

## 场景 B：限流关闭（测后端真实容量）

用于给第四节调阈值提供输入。

```bash
# 1. 把 VoucherOrderController 上的 @RateLimit qps 临时改为 100000（等效关闭）
# 2. 重启应用
# 3. 重跑场景 A 的 2-4 步（压测端不用改）
# 4. 【务必改回 qps = 50 并重启】
```

⚠️ **改完必须还原**。忘了还原，线上秒杀就是完全不设防。

## 场景 C：ab 交叉验证（排除压测端瓶颈）

JMeter 本身也会成为瓶颈。用 C 实现的 ab 复核：

```bash
# 先取一个 token（走正常登录流程，不要绕过）
P=13800138000
redis-cli DEL "ratelimit:sms:cooldown:$P" "ratelimit:sms:daily:$P"
curl -s -o /dev/null -X POST "http://127.0.0.1:8081/user/code?phone=$P"
CODE=$(redis-cli GET "login:code:$P")
TOKEN=$(curl -s -X POST http://127.0.0.1:8081/user/login -H 'Content-Type: application/json' \
        -d "{\"phone\":\"$P\",\"code\":\"$CODE\"}" | sed -n 's/.*"data":"\([^"]*\)".*/\1/p')

: > /tmp/empty_post.txt
ab -n 3000 -c 1000 -k -p /tmp/empty_post.txt -T "application/json" \
   -H "authorization: $TOKEN" \
   http://127.0.0.1:8081/voucher-order/seckill/105
```

> ⚠️ **ab 全程复用同一个 token**，所以它最多只能创建 1 单（一人一单约束），其余全部落在拒绝路径。这正是它的定位——衡量**拒绝路径**的吞吐上限，**不能**当作下单容量使用（见 4.3）。

---

# 三、结果审阅

## 3.1 第一步：对账（正确性）——**先看这个**

QPS 只证明"快"，对账才证明"没错"。**四项全 PASS 才算有效结果**：

| 判定 | 含义 |
|---|---|
| 库存守恒 | DB 剩余库存 + 成交订单数 == 初始库存 |
| 一人一单 | 去重买家数 == 订单数 |
| 缓存一致 | Redis 库存 == DB 库存 |
| Redis Set | Redis 订单 Set 基数 == DB 订单数 |

```bash
./verify.sh 105 500
```

脚本会先等异步落库收敛（连续 3 次采样不变）再对账，避免读到中间态。

**对账 FAIL 时按这个顺序查**：

1. **是不是历史订单没清？** 看输出里 `DB剩余库存 + 成交订单数` 是否大于初始库存——若是，说明该券有上一轮的残留订单（`prepare.sh` 默认不删），加 `--reset-orders` 重跑
2. **是不是 Redis 订单数 ≠ DB 订单数？** 通常也是残留订单占了 `uk_active_user_voucher` 唯一索引，让 Redis 放行、DB 拒绝
3. **是不是两边都是 0？** 看应用侧 Redis 超时有没有把 Stream 消费端打断（见 1.4 第 5 条）
4. `redis-cli XPENDING stream.orders seckill-group` 看有没有堆积的未确认消息

> ⚠️ 若要手工删 `stream.orders` 做彻底重置，**必须先停应用**。消费组只在应用启动时创建，应用运行中删掉它会导致消费者持续报 `NOGROUP No such key 'stream.orders' or consumer group 'g1'`，异步落库全程失效（表现为订单数为 0、Redis 库存键为 nil）。正确顺序：停应用 → 清理 Redis → 启动应用。

## 3.2 第二步：吞吐（性能）

看 `report/index.html`：先确认 **`Err` 接近 0**，再看 Throughput 与 P99。

断言把 `200` 和 `429` 都算通过——429 是限流拒绝，属**预期结果**；只有连接错误和 5xx 才是真失败。

## 3.3 两个必须知道的误报/陷阱

**陷阱 1：冷启动会低 30%。** 同一配置连续跑，第一轮可能 4643 QPS、后几轮 6608 / 8059 / 7158。第一轮低是因为 JIT 编译和连接池预热都没完成。**绝不拿单次采样下结论**——很容易得出"性能下降 29%"这种错误判断。至少跑 3 轮，取预热后的值。

**陷阱 2：ab 的 `Failed requests` 会误报。** 明细若为 `Connect: 0, Receive: 0, Length: N, Exceptions: 0`，那是**响应体长度差异**（成功与售罄的 JSON 长度不同），ab 把"长度与首个响应不一致"计为失败，**不是真实错误**。只有 Connect / Receive / Exceptions 非 0 才是问题。

## 3.4 历史基线（用于对比）

| 场景 | 结果 | 说明 |
|---|---|---|
| 场景 A（Guava 限流，2026-08-20） | 110 单 / 2.38s | Python 客户端，请求被拖散在 2.38 秒里 |
| 场景 A′（Redis 限流，2026-09-17） | 64 单 / 1.0s | JMeter，令牌消耗窗口约 0.28s |
| 吞吐（ab 交叉验证） | 6579 → 6608 / 8059 / 7158 QPS | 多一次 Redis 往返在噪声内 |

> 两个下单数都严格符合 **容量 + 速率 × 令牌消耗窗口**：`50+50×1.3≈110`、`50+50×0.28≈64`。**差别在客户端，不在限流器**——这是一个很好用的判据：数对不上时，先用这个公式算一遍，能立刻分清是限流器变了还是压测端变了。

完整数据见 [`docs/perf-report.md`](../docs/perf-report.md)。

---

# 四、用压测结果调限流阈值

## 4.1 方法

1. 跑**场景 B**（关限流）测出该接口的容量 C
2. 阈值取 **0.6 ~ 0.8 × C**，留出余量给 GC 停顿、流量毛刺、同机其他接口争抢
3. 改回阈值后重跑**场景 A**，确认对账全 PASS 且 P99 可接受

## 4.2 逐接口：哪些能调、哪些不能

| 接口 | 能否用上述方法调 | 原因 |
|---|---|---|
| **秒杀 50** | ❌ **不要动** | 它是**业务漏斗宽度**，不是容量上限。1000 人抢 500 件本来就有 50% 注定失败，让失败请求在入口毫秒级返回、而不是去竞争 Redis 和 MySQL，这才是它的价值。调低不伤业务，调高才会 |
| **搜索 100** | ❌ **本机测不了** | 它是为保护 ES 而设的，但本机 ES 未启动，测出来的是 MySQL LIKE **降级路径**的容量——**比真实 ES 容量偏大**，拿它定阈值会设出一个 ES 扛不住的值。要调必须先起 ES 再测 |
| **支付 20** | ⚠️ 需自建脚本 | 压测脚手架未覆盖，且需要构造合法 orderId。支付链路瓶颈通常在外部渠道而非本服务 |
| **登录 5** | ❌ **不是数值问题** | 全局 5 QPS 意味着 5 个攻击者就能让全站用户登不上。它该改成**按手机号/IP 维度**（参照 `SmsRateLimiter` 的做法），而不是调大调小 |

## 4.3 三个陷阱

**1. localhost 单机测出的不是生产容量。** 压测端和服务端抢同一台机器的 CPU，Redis / MySQL 也是本机进程。这里的数字只能做**相对对比**（改前 vs 改后），不能当容量规划依据。

**2. 别拿"拒绝路径"的吞吐去定阈值。** ab 那 6,579 QPS 是 **3,000 个请求打向只有 500 库存的券**——无论库存何时售罄，**至少 83% 的请求走的是拒绝路径**（在 Lua 预检就返回，根本没走到下单）。真正该参考的是下单路径：场景 B3 的 1,000 并发里 500 单成交 / 1.107s ≈ **452 单/秒**。两者差一个数量级，用错会设出离谱的阈值。

**3. 阈值改完要重跑场景 A 验证，不能只测容量就上线。** 容量是"后端最多能扛多少"，阈值是"我允许多少进来"——两者之间还隔着限流器本身的正确性（比如令牌桶容量、突发窗口）需要实测确认。
