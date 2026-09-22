# 本地观测栈（Prometheus + Grafana）

抓本仓库 `:8081/actuator/prometheus` 的现成采集栈，用来把 `docs/perf-report.md`
里的"手测数字"换成可复现的 PromQL。**只跑在 127.0.0.1**，不对外。

## 前置条件

应用必须先起在宿主机上（观测栈不启动被测服务）：

```bash
mvn spring-boot:run          # 另一个终端
```

## 启动

```bash
cd observability
docker compose up -d
```

| 入口 | 地址 | 说明 |
|---|---|---|
| Prometheus | http://127.0.0.1:9090 | `/graph` 写 PromQL，`/alerts` 看规则，`/targets` 看抓取是否成功 |
| Grafana | http://127.0.0.1:3000 | 目录「点评后端」→ `点评后端 · 可观测总览` |
| 面板直链 | http://127.0.0.1:3000/d/dianping-overview | |

Grafana 口令默认 `admin/admin`，仅供本地。要改就用环境变量传，别写进 compose：

```bash
GRAFANA_PASSWORD='改成你的' docker compose up -d
```

## 停止 / 清理

```bash
docker compose down              # 停容器，保留时序数据
docker compose down -v           # 连 named volume 一起删（数据不可恢复）
```

时序数据放在 named volume（`observability_prometheus-data` / `observability_grafana-data`）
里，**不在工作区**，不会被 `git status` 看见，也不会有人把几百 MB 的 TSDB 误提交。

## 抓取了什么，为什么只有这一个 target

`prometheus.yml` 里只有一个业务 job：`host.docker.internal:8081/actuator/prometheus`。

- **没有抓 `/actuator/health`**。本地 ES 没起时它是 503，每 15s 一次的探测会把
  「非 2xx 占比」抬成一个与业务无关的常数基线。面板 JSON 里那条曲线额外用
  `uri!~"/actuator/.*"` 兜了一层，就是为了防有人以后手滑加回健康检查。
- **没有把 actuator 端点全开**。只暴露了 `health,prometheus`（见 `application.yaml`），
  所以这里能抓到的就是这些。
- `extra_hosts: host.docker.internal:host-gateway` 是为了同一份文件在 Linux 上也能抓
  到宿主应用；macOS 的 Docker Desktop 本来就内置解析。

## 面板

`grafana/dashboards/dianping-overview.json` 是**文件提供**的（`allowUiUpdates: false`）：

- 直接编辑这个 JSON，Grafana 30 秒内自动重载，不用重启容器；
- 在 UI 里改的东西会被文件覆盖回去。要固化 UI 上的调整，导出后写回仓库：

```bash
curl -s -u admin:admin \
  'http://127.0.0.1:3000/api/dashboards/uid/dianping-overview' \
  | jq '.dashboard | del(.id, .version, .meta)' > grafana/dashboards/dianping-overview.json
```

每个面板的 description 写的是**口径**（分母是什么、什么时候会误导人），不是图表说明。
所有查询与 `docs/backend-design.md` §10.1 保持一致，改了那边记得改这里。

## 已知坑

- **No data ≠ 0**。这些指标是懒注册的：`dianping_circuitbreaker_*` 要等第一次调用到
  被 `@CircuitBreaker` 保护的方法才出现，`dianping_seckill_precheck_total` 要等第一次
  真的进到 Lua 预检那一步（被限流挡在切面外层的请求不会注册它）。新启动的实例上
  面板空着是正常的。
- **`dianping_outbox_pending` 上限是 50**（`BATCH_LIMIT`）。它贴着 50 是"投递追不上写入"，
  不是"只有 50 条不严重"。gauge 读的是上一轮扫描的内存快照，抓取间隔调密也不会更准。
- **`$__rate_interval` 依赖 15s 抓取间隔**。数据源里显式写了 `timeInterval: "15s"`，
  改 `scrape_interval` 时两边都要改。
- **`rejected` 计数不知道当前生效的是哪个阈值**。限流阈值的动态覆盖发生在 `rate-limit.lua`
  里，切面拿不到生效值（日志打的是注解上的那个数）。半夜看到 `rejected` 涨起来，
  先 `GET /config` 对一下 `override / effectiveQps`，再决定是不是流量真的变大了 ——
  另一个常见答案是"白天有人改了阈值忘了改回来"。
- **`last_scan_age_seconds` 量的是调度心跳，不是扫描间隔**。心跳在门控之前打，
  所以把间隔调到合法上限 10s 它也还是毫秒级。这条正好是它的用途：`>30s` 说明
  **调度器本身没在跑**（线程池饿死、实例假死），而不是"间隔被人调大了"。
  间隔调大会影响的是 `oldest_age_seconds`（快照最长可能旧 10s），那是另一条规则。

## 验证状态（2026-09-19）

不接 Prometheus 也能核对查询，所以这套栈交付前是**对着真实抓取文本**核过的，不是照着代码猜的：

```bash
curl -s localhost:8081/actuator/prometheus > /tmp/scrape.txt
python3 check-queries.py grafana/dashboards/dianping-overview.json /tmp/scrape.txt
```

- **已实测存在**（本机起应用 + 打了几种请求）：`dianping_cache_lookup_total` 的
  `hit / hit_plain / null_hit / not_cached / miss_db_found / stale_served` 六档、
  `dianping_cache_rebuild_total{outcome="claimed"}`、`dianping_rate_limit_total` 的
  `allowed / rejected`、`dianping_seckill_precheck_total{reason="out_of_stock"}`、
  `dianping_circuitbreaker_{state,failures,fallback_total,transition_total}`（ES 停着跑
  `/shop/search` 就能凑出 CLOSED→OPEN）、`dianping_outbox_{pending,oldest_age_seconds,last_scan_age_seconds}`、
  `executor_*{name="cache-rebuild"}`，以及 `uri` 标签的真实形状
  （`/voucher-order/seckill/{id}`、`/shop/{id}`、`/shop/search`）和 69 个 `le` 桶。
- **已实测存在（ES_SYNC 事件）**：`dianping_outbox_event_total{result="failed",type="ES_SYNC"}`。
  应用起着、ES 停着时改一次商铺就必出这条 —— 顺带说明这个计数器的"值"比"名字"更有用：
  `type` 标签是这一档事件的健康度入口。`result="sent"` 要 ES 起着才能观测。
- **动态扫描间隔靠单测锁，不靠面板**：`TransactionOutboxPublisherScanIntervalTest` 断言
  「被门控跳过的那一轮一次 DB 都不碰，但心跳照打」。实机只测了投递延迟（间隔 10000ms 时
  一条探针事件 8.87s 被拿走、200ms 时 0.27s，见 `docs/backend-design.md` §5.4），
  没有专门去抓 `last_scan_age_seconds` 的曲线 —— 上面那条"心跳与间隔解耦"是按代码读出来的。
- **只核过名字与标签、值还是空的**：`dianping_seckill_precheck_total{reason="ok"}`
  （要真下成一单）、`dianping_rate_limit_total{outcome="unavailable_rejected"}`
  （要真的把 Redis 停掉）、`dianping_outbox_event_total` 的其余 `type/result` 组合、
  `dianping_outbox_stuck_recovered_total`。
  这几个不制造对应故障就出不来，而面板上它们本来就该是空的 —— 恰恰是"出现即有问题"的那几个。
- **`dianping_seckill_{consumer,reconcile}_total` 比上面那一档更弱一档**：它们没进过
  真实抓取，只有测试里走通的证据 —— 单测与真机测试断言的是 Micrometer 计数器本身
  （`SimpleMeterRegistry` 里按 result 标签取到的值），而 Prometheus 侧的名字
  （`dianping.seckill.consumer` → `dianping_seckill_consumer_total`）是按既有约定推的，
  与 `dianping_outbox_event` → `dianping_outbox_event_total` 同源但没被实测核对过。
  起一次应用、下一单跑到落库，再跑 `check-queries.py` 就能把这一档补上。
  各标签值的含义见 `docs/backend-design.md` §10.1。
- **没跑过的**：`docker compose up` 本身。写这套文件时本机的 Docker daemon 没在跑，
  所以 Prometheus / Grafana 两个容器的实际启动、`/targets` 是否 UP、Grafana 是否真的加载了
  这个 JSON，都属于"待你起一次"的状态。能静态验证的都验证了：compose 过了
  `docker compose config`，JSON 过了结构与网格重叠检查，YAML 过了 `safe_load`，
  告警规则数与 §10.1 的口径逐条对过。

## 不含

- **Alertmanager**：`prometheus/alerts/dianping.yml` 定义了 10 条规则并会真的评估，
  但没有推送出口 —— "告警"在这里指 `/alerts` 页面变红和面板上的阈值线。
  要推送就加一个 alertmanager 服务并填 `prometheus.yml` 的 `alerting.alertmanagers`。
- **日志/追踪后端**：traceId 目前只在单进程的日志里（Phase 2），没有 Loki/Tempo/Jaeger，
  也没有跨进程传播。想上分布式追踪是另一个决定，不是这套栈的缺件。
