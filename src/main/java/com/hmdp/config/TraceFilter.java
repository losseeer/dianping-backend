package com.hmdp.config;

import com.hmdp.utils.TraceContext;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * HTTP 入口的 traceId 过滤器 —— 整条链路的起点。
 *
 * 【为什么用 Filter 而不是 Interceptor】Filter 在 Interceptor 之外，
 * 意味着 401（LoginInterceptor 拦下的）、参数校验失败、全局异常这些
 * "根本没走到业务代码"的请求也有一行可查的日志。
 * Phase 1 的教训在这里反过来用：/actuator/prometheus 那次差点因为
 * 白名单漏配而全 401；Filter 不受 Interceptor 白名单影响，天生不会犯这个错。
 *
 * 【为什么允许上游传 id】网关、前端、压测脚本带上 X-Trace-Id 就能把
 * 服务端日志和调用方日志并到一条链上；不传则服务端自己生成。
 * 传进来的值一律过 TraceContext.accept 的白名单，防止日志伪造。
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class TraceFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        try (TraceContext.Scope ignored = TraceContext.enter(request.getHeader(TraceContext.HEADER))) {
            // 回写响应头：客户端拿到它才能把"我这次请求为什么慢"精确指到服务端那几行日志，
            // 否则只能拿时间戳去日志里捞。
            response.setHeader(TraceContext.HEADER, TraceContext.current());
            filterChain.doFilter(request, response);
        }
    }
}
