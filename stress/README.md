# 秒杀压测 · JMeter 脚手架

对接口 `POST http://127.0.0.1:8081/voucher-order/seckill/{voucherId}` 做 1000 并发压测。

| 文件 | 作用 |
|---|---|
| `prepare.sh` | 刷新活动窗口 + 重置库存 + 注入 1000 个登录会话（生成 `tokens.csv`），**每次压测前跑一次** |
| `tokens.csv` | 1000 行 `token,userId`（无表头），JMeter 参数化用，由 prepare.sh 生成 |
| `seckill.jmx` | JMeter 脚本，GUI 直接 `File → Open` |
| `verify.sh` | 压测后四维度对账（库存守恒 / 一人一单 / 缓存一致） |
| `result.jtl`、`report/` | 运行产物 |

---

## 一、前置准备（必须先做）

```bash
cd /Users/dedsecczk/Dev/dianping/stress
./prepare.sh 105 1000 500      # 券105、1000用户、库存500
```

三件事：
1. 刷新 `tb_seckill_voucher` 的 `begin_time/end_time/stock` —— **券 105 原 end_time 是 2026-08-20，已过期，不改会全部返回"秒杀已经结束"**
2. 从 `tb_user` 取 1000 个**真实** userId，注入 Redis 会话 `login:token:tkN`（Hash：`id/nickName/icon`）—— 登录接口有 `@RateLimit(qps=5)`，压测绝不能走它
3. 清理上一轮残留的 Redis 秒杀键

确认 SpringBoot（:8081）和 Redis 已启动。

---

## 二、GUI 配置（已生成 jmx，Open 即可；下面是手动搭时的对照清单）

`File → Open → stress/seckill.jmx`，树结构如下：

```
快评-秒杀压测 (Test Plan)
└─ 秒杀线程组(1000并发) (Thread Group)
   ├─ HTTP请求默认值 (HTTP Request Defaults)
   ├─ token参数化 (CSV Data Set Config)
   ├─ HTTP信息头管理器 (HTTP Header Manager)
   ├─ 集合点 (Synchronizing Timer)          ← 关键
   └─ POST /voucher-order/seckill/105 (HTTP Request)
      ├─ 提取success字段 (Regex Extractor)
      └─ 响应码断言 (Response Assertion)
   ├─ 汇总报告 (Summary Report)
   └─ 写JTL结果 (Simple Data Writer)
```

逐项配置值：

| 元件 | 关键配置 | 为什么这么配 |
|---|---|---|
| **User Defined Variables** | `HOST=127.0.0.1` `PORT=8081` `VOUCHER_ID=105` `THREADS=1000` `CSV_PATH=/Users/.../stress/tokens.csv` | 集中管理，改一处即可切换场景。**CSV 与 JTL 都用绝对路径**——JMeter 的相对路径基于启动目录，GUI 从 `bin/` 启动会找不到文件 |
| **Thread Group** | `Number of Threads = ${THREADS}`(1000)、**Ramp-up = 0**、Loop Count = 1 | Ramp-up=0 才是瞬时并发；渐进爬坡测不出秒杀形态 |
| **CSV Data Set Config** | Filename `tokens.csv`；Variable Names `token,userId`；Delimiter `,`；**Recycle on EOF = False**；**Stop thread on EOF = True**；Sharing mode = All threads | 每个线程拿一个独立 token，对应一个真实用户 |
| **HTTP Header Manager** | `authorization: ${token}` | `RefreshTokenInterceptor` L64 读这个 header |
| **Synchronizing Timer** | `Number of Simultaneous Users = 1000`，Timeout = 0 | **集合点**：1000 线程全部就绪才同一瞬间放行，等价于 Python 版的 `asyncio.Event` 门闸 |
| **HTTP Request** | Method `POST`；Path `/voucher-order/seckill/${VOUCHER_ID}`；勾选 **Use KeepAlive** | KeepAlive 规避 macOS `somaxconn=128` 的 SYN 重传退避 |
| **Response Assertion** | Field = Response Code；Pattern = `200` 和 `429`（勾选 **Or**） | 429 是限流拒绝，属**预期结果**；只有连接错误/5xx 才算真失败 |
| **Summary Report** | 仅 GUI 查看 | 正式压测前建议右键 **Disable**，或直接用命令行跑 |

> ⚠️ **线程数改了必须同步改 SyncTimer.groupSize**，两者必须相等，否则线程会永久阻塞在集合点。

---

## 三、正式压测：用命令行，不要用 GUI

GUI 只用来搭脚本和单线程调试。**压测必须 CLI**，否则 JMeter 自身 GUI 渲染 + 监听器会吃掉大量 CPU/内存，测的是压测端不是服务端。

```bash
JM=/Users/dedsecczk/Downloads/apache-jmeter-5.6.3
cd /Users/dedsecczk/Dev/dianping/stress
JVM_ARGS="-Xms2g -Xmx4g" $JM/bin/jmeter -n -t seckill.jmx -l result.jtl -e -o ./report
open report/index.html
```

覆盖变量（不用改 jmx）：

```bash
$JM/bin/jmeter -n -t seckill.jmx -JTHREADS=2000 -l result.jtl -e -o ./report
```

---

## 四、JMeter 必须改的默认参数（**最大的坑**）

`httpclient4.max_per_route` 默认是 **2** —— 1000 个线程共享到同一 host 的 2 条连接，全部排队建连，你测出来的将是"建连开销"而非服务端能力。

JMeter 在 `/Users/dedsecczk/Downloads/apache-jmeter-5.6.3`，**已改好**（原文件备份为 `bin/jmeter.properties.bak.20260907`）：

```properties
httpclient4.max_per_route=1000
httpclient4.time_to_live=60000
```

> 5.6.3 的 jmeter.properties 里原本只有被注释掉的 `time_to_live`，`max_per_route` 根本没列出来——默认值 2 是写死在代码里的，所以**必须显式追加**才生效。

其余注意事项：

- **关掉 View Results Tree**：高并发下会把内存吃光，只保留 Simple Data Writer 写 JTL
- **同一台 macOS 上跑 JMeter + SpringBoot** 会 CPU 争抢，结果只能做**相对对比**，绝对数字要打折
- **macOS `somaxconn=128`** 依然存在：靠 KeepAlive 复用连接基本可消除

---

## 五、对账（压测后必跑）

```bash
./verify.sh 105 500
```

输出四项判定：库存守恒（DB库存+订单=500）、一人一单（买家数=订单数）、Redis 库存=DB 库存、Redis Set 基数=DB 订单数。脚本会自动等待异步落库收敛（连续 3 次采样不变）。

---

## 六、建议跑三轮，每轮回答一个明确问题

| 轮次 | 配置 | 回答什么 |
|---|---|---|
| **J1** | 限流 50 QPS（生产配置，默认） | 削峰比例、后端真实承压 |
| **J2** | 关限流（`@RateLimit` 调到 100000）+ 默认连接池 | 与 Python 版 B1(191 QPS) 对照 |
| **J3** | 关限流 + `max_per_route=1000` + KeepAlive | 与 Python B3(903)、ab(6579) 交叉验证 |

J3 落在 3000~6000 区间 → 证实"Python 是客户端瓶颈、ab 才是服务端上限"；若也接近 900 → 说明 903 可能不只是 Python 的锅，这本身也是个值得写进报告的新发现。

每轮之间重跑 `./prepare.sh` 重置状态。

---

## 七、面试怎么讲这组数据

三个客户端（Python asyncio 903 / JMeter ? / ab 6579）测同一接口的三组数据，把"压测端是瓶颈"从**猜测**变成**三种工具验证过的结论**。

- **6579 QPS 必须加限定**：是**库存售罄拒绝路径**的吞吐，不是下单吞吐；且是**临时关闭限流**测的理论上限；**localhost 单机**，不能当生产容量
- **被问"库存卖完了还测什么"**：秒杀的真实形态就是绝大部分请求必然失败（1000 人抢 500 件，50% 注定抢不到）。拒绝路径决定失败用户多久拿到响应，进而决定是否疯狂重试把系统打爆
