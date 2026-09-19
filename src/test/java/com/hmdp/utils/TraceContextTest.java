package com.hmdp.utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * traceId 上下文测试。
 *
 * <p>
 * 【为什么这些用例值得写】traceId 是靠 ThreadLocal 手工接力的，两个失败模式都很安静：
 * 一是漏 clear，线程被复用后下一个请求会顶着上一个人的 id 打日志——日志看起来完全正常，
 * 但指向另一条链路，排查时比没有 id 更害人；二是外部传进来的 id 不校验，
 * 一个带换行的请求头就能往日志里伪造整行。这两条都有用例钉住。
 *
 * <p>
 * 不打 Spring 上下文，MDC 本身就是静态的，用例之间靠 Scope 关闭 + afterEach 兜底保证互不污染。
 */
@DisplayName("TraceContext 链路上下文")
class TraceContextTest {

    private static final String GENERATED = "[0-9a-f]{16}";

    @AfterEach
    void clearMdc() {
        MDC.remove(TraceContext.TRACE_ID);
    }

    @Test
    @DisplayName("enter 写入 traceId，close 之后清空")
    void enterSetsAndScopeClears() {
        assertNull(TraceContext.current());
        String inside;
        try (TraceContext.Scope ignored = TraceContext.enter()) {
            inside = TraceContext.current();
            assertNotNull(inside);
        }
        assertNull(TraceContext.current(), "Scope 关闭后必须清干净，否则线程复用会串号");
        assertTrue(inside.matches(GENERATED), "生成的 id 应为 16 位十六进制，实际=" + inside);
    }

    @Test
    @DisplayName("合法的入站 traceId 原样沿用")
    void legalInboundIdIsReused() {
        try (TraceContext.Scope ignored = TraceContext.enter("stress-run-0919_1")) {
            assertEquals("stress-run-0919_1", TraceContext.current());
        }
    }

    @Test
    @DisplayName("非法入站 traceId 一律弃用：日志注入、超长、太短、空串")
    void illegalInboundIdIsRejected() {
        String[] illegal = {
                "abcdefgh\n2026-09-19 12:00:00  INFO fake-log-line",
                String.join("", Collections.nCopies(65, "x")),
                "abc",
                "",
                "has space",
                "中文id000000000",
        };
        for (String candidate : illegal) {
            try (TraceContext.Scope ignored = TraceContext.enter(candidate)) {
                String accepted = TraceContext.current();
                assertTrue(accepted.matches(GENERATED),
                        "非法入站值应被换成新生成的 id，入参=[" + candidate + "] 实际=" + accepted);
                assertFalse(accepted.contains("\n"), "traceId 里绝不能出现换行");
            }
        }
        try (TraceContext.Scope ignored = TraceContext.enter(null)) {
            assertTrue(TraceContext.current().matches(GENERATED));
        }
    }

    @Test
    @DisplayName("嵌套 Scope：内层 close 只恢复外层，不清空")
    void nestedScopeRestoresOuterTrace() {
        try (TraceContext.Scope outer = TraceContext.enter("outertraceid0001")) {
            try (TraceContext.Scope inner = TraceContext.enter("innertraceid0001")) {
                assertEquals("innertraceid0001", TraceContext.current());
            }
            assertEquals("outertraceid0001", TraceContext.current(),
                    "在已有链路里套一层临时作用域之后，不能把外层抹掉");
        }
        assertNull(TraceContext.current());
    }

    @Test
    @DisplayName("wrap 把 traceId 带进线程池，用完不在线程上留痕")
    void wrapPropagatesIntoExecutorAndLeavesNothingBehind() throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            AtomicReference<String> seenByWrapped = new AtomicReference<>();
            Future<?> first;
            try (TraceContext.Scope ignored = TraceContext.enter("parenttraceid001")) {
                first = pool.submit(TraceContext.wrap(capture(seenByWrapped)));
            }
            first.get(5, TimeUnit.SECONDS);
            assertEquals("parenttraceid001", seenByWrapped.get(),
                    "线程池里的任务日志必须带上触发它的那次请求的 id");

            AtomicReference<String> seenByPlain = new AtomicReference<>("unset");
            pool.submit(capture(seenByPlain)).get(5, TimeUnit.SECONDS);
            assertNull(seenByPlain.get(), "同一条池线程复用后不该残留上一个任务的 traceId");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("调用线程自己执行 wrap 过的任务，结束后 traceId 还在")
    void wrapKeepsCallerTraceWhenTaskRunsInline() {
        AtomicReference<String> seen = new AtomicReference<>();
        try (TraceContext.Scope ignored = TraceContext.enter("callerttraceid01")) {
            // 模拟 CallerRuns 式的原地执行：任务在调用线程上跑，不能把调用方的 id 一起清掉
            TraceContext.wrap(capture(seen)).run();
            assertEquals("callerttraceid01", TraceContext.current());
        }
        assertEquals("callerttraceid01", seen.get());
    }

    @Test
    @DisplayName("无上下文时 wrap 原样返回，不多包一层")
    void wrapIsNoOpWithoutParentTrace() {
        Runnable task = () -> {
        };
        assertEquals(task, TraceContext.wrap(task));
    }

    @Test
    @DisplayName("消息头只认字符串，byte[] 之类的值当作没有上游")
    void headerAcceptsOnlyCharSequence() {
        Map<String, Object> headers = new HashMap<>();
        headers.put(TraceContext.HEADER, "fromheader000001");
        try (TraceContext.Scope ignored = TraceContext.enterHeader(headers)) {
            assertEquals("fromheader000001", TraceContext.current());
        }

        Map<String, Object> bytes = Collections.singletonMap(TraceContext.HEADER, new byte[]{1, 2, 3});
        try (TraceContext.Scope ignored = TraceContext.enterHeader(bytes)) {
            assertTrue(TraceContext.current().matches(GENERATED),
                    "byte[] 的 toString 是个内存地址，拿来当 id 比没有 id 更误导");
        }

        try (TraceContext.Scope ignored = TraceContext.enterHeader(null)) {
            assertTrue(TraceContext.current().matches(GENERATED));
        }
    }

    private static Runnable capture(AtomicReference<String> holder) {
        return () -> holder.set(TraceContext.current());
    }
}
