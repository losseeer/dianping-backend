package com.hmdp.config;

import com.hmdp.utils.TraceContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HTTP 入口 traceId 测试。
 *
 * <p>
 * 【盯住两件事】一是请求结束必须把 MDC 清空——Tomcat 的 200 条工作线程是复用的，
 * 漏清等于下一个请求继承上一个的链路；二是响应头要回写，否则调用方（联调、压测脚本）
 * 只能拿时间戳去日志里捞，等于这个功能对外没交付。
 *
 * <p>
 * 用 Mock 的 request/response 直接调过滤器，不起 Spring 上下文：这里测的是我们自己那几行，
 * 不是 Boot 的过滤器注册。
 */
@DisplayName("TraceFilter 请求入口")
class TraceFilterTest {

    private final TraceFilter filter = new TraceFilter();

    @Test
    @DisplayName("不带请求头时自己生成，并在响应头回写同一个 id")
    void generatesAndEchoesTraceId() throws Exception {
        AtomicReference<String> duringRequest = new AtomicReference<>();
        MockHttpServletResponse response = handle(new MockHttpServletRequest(),
                chainRecording(duringRequest));

        assertNotNull(duringRequest.get(), "请求处理过程中 MDC 里必须有 traceId");
        assertEquals(duringRequest.get(), response.getHeader(TraceContext.HEADER),
                "响应头回写的必须是同一个 id");
        assertTrue(duringRequest.get().matches("[0-9a-f]{16}"));
        assertNull(MDC.get(TraceContext.TRACE_ID), "请求结束后必须清空");
    }

    @Test
    @DisplayName("上游（网关/压测脚本）传的合法 id 全程沿用")
    void reusesInboundTraceId() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(TraceContext.HEADER, "stress-20260919-a1");
        AtomicReference<String> duringRequest = new AtomicReference<>();

        MockHttpServletResponse response = handle(request, chainRecording(duringRequest));

        assertEquals("stress-20260919-a1", duringRequest.get());
        assertEquals("stress-20260919-a1", response.getHeader(TraceContext.HEADER));
    }

    @Test
    @DisplayName("带换行的请求头不会伪造出日志行")
    void rejectsHeaderThatCouldForgeLogLines() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(TraceContext.HEADER, "aaaaaaaa\n2026-09-19 12:00:00  INFO 伪造的登录成功日志");
        AtomicReference<String> duringRequest = new AtomicReference<>();

        MockHttpServletResponse response = handle(request, chainRecording(duringRequest));

        assertTrue(duringRequest.get().matches("[0-9a-f]{16}"),
                "非法入参应被换成新生成的 id，实际=" + duringRequest.get());
        String echoed = response.getHeader(TraceContext.HEADER);
        assertFalse(echoed.contains("\n"), "响应头里不允许出现被污染的原始值");
    }

    @Test
    @DisplayName("下游抛异常也要清空 MDC")
    void clearsTraceWhenRequestFails() {
        MockFilterChain chain = new MockFilterChain() {
            @Override
            public void doFilter(ServletRequest request, ServletResponse response) {
                throw new IllegalStateException("业务炸了");
            }
        };
        Exception expected = null;
        try {
            handle(new MockHttpServletRequest(), chain);
        } catch (Exception e) {
            // 异常照常往上抛，交给全局异常处理器；这里只关心 MDC 有没有被清掉
            expected = e;
        }
        assertNotNull(expected);
        assertNull(MDC.get(TraceContext.TRACE_ID));
    }

    private MockHttpServletResponse handle(MockHttpServletRequest request, FilterChain chain)
            throws Exception {
        MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, chain);
        return response;
    }

    private MockFilterChain chainRecording(AtomicReference<String> holder) {
        return new MockFilterChain() {
            @Override
            public void doFilter(ServletRequest request, ServletResponse response) {
                holder.set(TraceContext.current());
            }
        };
    }
}
