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

## 一、跑一轮

```bash
cd stress            # 在仓库根目录执行。必须先 cd，相对路径按启动目录解析

# 1. 起应用（另开终端，JDK 8）—— mvn spring-boot:run  → :8081
#    依赖 MySQL / Redis / RabbitMQ；ES 不用起（检索自动降级到 MySQL LIKE）

# 2. 准备：券 105、1000 用户、库存 500
./prepare.sh 105 1000 500

# 3. 压测
JM=~/Downloads/apache-jmeter-5.6.3                        # 改成你的 JMeter 解压目录
rm -rf report                                             # -o 要求目录不存在或为空
JMETER_COMPLETE_ARGS=true JVM_ARGS="-Xms2g -Xmx4g -server" \
  $JM/bin/jmeter -n -q jmeter.properties -t seckill.jmx -l result.jtl -e -o ./report
open report/index.html

# 4. 对账（压测后必跑）
./verify.sh 105 500
```

`prepare.sh` 做的三件事：刷新活动窗口（券 105 原 `end_time` 已过期，不改会全部返回"秒杀已经结束"）、从 `tb_user` 取 1000 个**真实** userId 直写 Redis 会话（登录接口有 `@RateLimit(qps=5)`，压测不能走它）、清理上轮 Redis 残留键。

> 清理项里包含 `ratelimit:api:*`——`@RateLimit` 已是 Redis 分布式令牌桶，上一轮打空的桶若留到这一轮会压制场景 A 的冷启动突发，"成功下单数"就会因为与本次改动无关的原因漂移。

---

## 二、看什么结果

### 对账（`verify.sh`）

QPS 只证明"快"，对账才证明"没错"。**四项全 PASS 才算有效结果**：

| 判定 | 含义 |
|---|---|
| 库存守恒 | DB 剩余库存 + 成交订单数 == 初始库存 |
| 一人一单 | 去重买家数 == 订单数 |
| 缓存一致 | Redis 库存 == DB 库存 |
| Redis Set | Redis 订单 Set 基数 == DB 订单数 |

脚本会先等异步落库收敛（连续 3 次采样不变）再对账，避免读到中间态。

### 吞吐（`report/index.html`）

断言把 `200` 和 `429` 都算通过——429 是限流拒绝，属**预期结果**；只有连接错误和 5xx 才是真失败。所以看报告时先确认 `Err` 接近 0，再看 Throughput / P99。

### 三轮对照

每轮之间重跑 `./prepare.sh 105 1000 500` 重置状态：

| 轮次 | 配置 | 回答什么 |
|---|---|---|
| **J1** | 限流 50 QPS（默认生产配置） | 削峰比例、后端在保护下的承压表现 |
| **J2** | 关限流（`@RateLimit` 调到 100000） | 后端不限流时的吞吐上限 |
| **J3** | 关限流 + 连接池调优 + KeepAlive | 排除压测端瓶颈后，后端的真实容量 |

J3 明显高于 J2 → 瓶颈在压测端；两者接近 → 后端已饱和。历史数据见 [`docs/perf-report.md`](../docs/perf-report.md)。

---

## 三、必须先知道的坑

**1. `JMETER_COMPLETE_ARGS=true` 不能省。** JMeter 启动脚本无条件注入 `-XX:+UseG1GC`，本机 JDK `1.8.0_491` 判其为实验特性，直接退出：

```
Error: VM option 'UseG1GC' is experimental and must be enabled via -XX:+UnlockExperimentalVMOptions.
```

该变量会清空脚本自带的 ARGS，让 `JVM_ARGS` 完全接管。G1 在 JDK 8 上非必需，默认 ParallelGC 对压测端吞吐更稳。

**2. 相对路径按「启动目录」解析，不是 JMX 所在目录。** 所以必须先 `cd stress` 再启动 JMeter（GUI 同理），否则报 `File tokens.csv must exist and be readable`。

**3. `-o ./report` 要求目标目录不存在或为空。** 重复跑会报 `Cannot write to '.../report' as folder is not empty`，先 `rm -rf report`。

**4. 改线程数必须同步改 `SyncTimer.groupSize`**（jmx 里写死 1000，未参数化）。两者不等会让线程永久阻塞在集合点。

### 已做的调优

`httpclient4.max_per_route` 默认是 **2**——1000 线程共享 2 条连接排队建连，测出来的是建连开销而非服务端能力。该覆盖项随仓库分发在 [`jmeter.properties`](jmeter.properties)，命令里的 `-q` 即加载它，**不需要改 JMeter 安装目录**。

jmx 里另有两处对应调优：HTTP 请求勾选 **KeepAlive**（规避 macOS `somaxconn=128` 的 SYN 重传退避）；**集合点**等 1000 线程全部就绪后同一瞬间放行，制造真正的瞬时并发而非渐进爬坡。

**本机同时跑 JMeter 和 SpringBoot 会 CPU 争抢**，结论只做相对对比，绝对数字要打折。
