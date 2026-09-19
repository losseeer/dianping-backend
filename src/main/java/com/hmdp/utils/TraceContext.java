package com.hmdp.utils;

import org.slf4j.MDC;

import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * 链路追踪上下文 —— 【八股:traceId 为什么要自己写而不上 Sleuth?】
 *
 * 【要解决的问题】本项目一次业务经常跨线程：
 * HTTP 线程 → Redis Stream 消费线程 → RabbitMQ 消费线程 → @Scheduled 发布线程 → 缓存重建线程池。
 * 日志默认只带线程名，跨线程之后同一次请求的日志就散成五截，只能靠时间和订单号猜。
 *
 * 【原理】SLF4J 的 MDC 是一个 ThreadLocal<Map>，logback 用 %X{key} 取值。
 * 关键点：ThreadLocal 不会自动跨线程，所以每一个线程切换点都必须显式接力，
 * 漏一个点链路就断在那里（这正是 Sleuth 做的事——它包装线程池和 MQ 组件）。
 * 这里不引入 Sleuth：它要改 Tomcat/Redis/Rabbit/线程池四类组件，
 * 而本项目的异步入口只有 5 个，手写接力的代码量更小、且每一步都可解释。
 *
 * 【三条纪律】
 * 1. 谁开启谁关闭：enter() 返回 Scope，用 try-with-resources 保证 clear 一定执行。
 *    线程是复用的（Tomcat 200 个、消费线程常驻），漏 clear = 下一个请求读到上一个人的 traceId，
 *    这种错比没有 traceId 更糟糕——日志会指向一条完全不同的链路。
 * 2. 外部传进来的 id 必须校验：见 accept()。
 * 3. 只放 traceId，不放 userId/订单号：那些是"查某笔业务的全部日志"的维度，
 *    直接写在日志文本里就行；MDC 里每个 key 都会出现在每一行日志上，越精简越好。
 */
public final class TraceContext {

    /** MDC 的 key，同时也是 logback pattern 里的 %X{traceId} */
    public static final String TRACE_ID = "traceId";

    /** HTTP 请求/响应头与 RabbitMQ 消息头用的字段名（三处共用一个常量，避免拼写分叉） */
    public static final String HEADER = "X-Trace-Id";

    /**
     * 合法 traceId：只允许字母数字与 -_，长度 8~64。
     *
     * 【为什么要卡字符集】traceId 是从请求头读来的外部输入，会原样打进日志。
     * 不校验的话 "abc\n2026-09-19 12:00:00 INFO ... 假装自己是日志" 就能伪造日志行（log forging），
     * 塞超长字符串则能把日志盘刷爆。长度下限 8 是为了让"随便传个 x"没有意义。
     */
    private static final Pattern LEGAL = Pattern.compile("^[A-Za-z0-9_-]{8,64}$");

    private TraceContext() {
    }

    /** 新生成一个 traceId：UUID 去横线取前 16 位。够短好 grep，冲突概率对单机日志无关紧要 */
    public static String newTraceId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 16);
    }

    public static String current() {
        return MDC.get(TRACE_ID);
    }

    /** 外部传来的 id：合法就沿用（这样上下游能共用一条链路），非法则新生成 */
    public static String accept(String inbound) {
        return (inbound != null && LEGAL.matcher(inbound).matches()) ? inbound : newTraceId();
    }

    /** 开一段链路作用域，close 时恢复进入前的值（可能是 null，也可能是外层链路的 id） */
    public static Scope enter(String inbound) {
        String previous = current();
        String traceId = accept(inbound);
        MDC.put(TRACE_ID, traceId);
        return new Scope(previous);
    }

    /** 没有上游 id 时（定时任务、消费到一条老消息）自己起一个 */
    public static Scope enter() {
        return enter(null);
    }

    /**
     * 从 RabbitMQ 消息头里接棒。headers 的值是 Object（对方可能塞任何类型），
     * 只认字符串，其它一律当作没有上游 id —— 不去 toString() 是因为
     * byte[] 的 toString 得到 "[B@1a2b"，那会变成一个查无此人的 id，比没有 id 更误导。
     */
    public static Scope enterHeader(Map<String, Object> headers) {
        Object raw = headers == null ? null : headers.get(HEADER);
        return enter(raw instanceof CharSequence ? raw.toString() : null);
    }

    public static final class Scope implements AutoCloseable {
        private final String previous;

        private Scope(String previous) {
            this.previous = previous;
        }

        @Override
        public void close() {
            if (previous == null) {
                MDC.remove(TRACE_ID);
            } else {
                MDC.put(TRACE_ID, previous);
            }
        }
    }

    /**
     * 把当前线程的 traceId 带进线程池任务里。
     *
     * 【为什么在提交处包而不是包线程工厂】MDC 是 ThreadLocal，池化线程活得很比单次任务长，
     * 想在 Runnable 里读调用方上下文必须在提交那一刻抓快照；包 ThreadFactory 只能改线程名，改不了 MDC。
     *
     * 【为什么用恢复而不是清空】任务可能跑在调用线程上（比如队列满了走 CallerRuns，
     * 或本项目的缓存重建被拒绝后原地返回），clear 会把调用方自己的 traceId 一起清掉。
     */
    public static Runnable wrap(Runnable task) {
        String parent = current();
        if (parent == null) {
            return task;
        }
        return () -> {
            String previous = current();
            MDC.put(TRACE_ID, parent);
            try {
                task.run();
            } finally {
                if (previous == null) {
                    MDC.remove(TRACE_ID);
                } else {
                    MDC.put(TRACE_ID, previous);
                }
            }
        };
    }
}
