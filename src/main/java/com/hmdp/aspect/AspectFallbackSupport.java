package com.hmdp.aspect;

import com.hmdp.dto.Result;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.util.ClassUtils;

import java.lang.reflect.Method;

/**
 * 切面公共工具 —— 方法签名解析 + 反射调用同类降级(fallback)方法。
 * CircuitBreakerAspect / RateLimitAspect 之前各持有一份逐字相同的实现，收敛于此。
 */
@Slf4j
public final class AspectFallbackSupport {

    private AspectFallbackSupport() {
    }

    /**
     * 方法标识（全限定类名.方法名），作为限流器/熔断器的 key
     *
     * <p>
     * 【为什么必须用全限定名】这个字符串现在是 Redis 的 key
     * （{@code ratelimit:api:com.hmdp.controller.UserController.login}），
     * 是个<strong>跨实例共享的命名空间</strong>。用简单类名的话，将来加一个
     * {@code admin.UserController} 就会和现有的 UserController 静默共用同一个
     * 令牌桶——限流阈值互相干扰，而且响应和日志里都看不出来。
     *
     * <p>
     * 【为什么还要 getUserClass】拿到的可能是 CGLIB 增强类，其类名带每 JVM
     * 生成的随机后缀。若不剥离，每个实例会算出不同的 key —— 分布式限流会
     * 静默退化成单机限流，而这恰恰是本次改造要修的问题。它也是"单实例测试
     * 全绿、多实例才出问题"的典型。
     */
    public static String getMethodName(ProceedingJoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        return ClassUtils.getUserClass(method.getDeclaringClass()).getName() + "." + method.getName();
    }

    /**
     * 反射调用与原方法同参数列表的降级方法。
     *
     * 【八股：降级方法的要求】
     * 1. 必须和原方法在同一个类中
     * 2. 方法签名（返回值、参数列表）必须一致
     * 3. 访问修饰符不影响（private也能通过反射调用）
     * 4. 降级方法上不要再加同类切面注解（否则又被拦截）
     *
     * @param unavailableResult 找不到/执行失败降级方法时返回的兜底响应
     */
    public static Object invokeFallback(ProceedingJoinPoint joinPoint, String fallbackName,
                                        Result unavailableResult) throws Throwable {
        try {
            MethodSignature signature = (MethodSignature) joinPoint.getSignature();
            Method fallbackMethod = signature.getDeclaringType().getDeclaredMethod(
                    fallbackName, signature.getParameterTypes());
            fallbackMethod.setAccessible(true);
            Object target = joinPoint.getTarget();
            return fallbackMethod.invoke(target, joinPoint.getArgs());
        } catch (NoSuchMethodException e) {
            log.error("降级方法不存在: {}", fallbackName, e);
            return unavailableResult;
        } catch (Exception e) {
            log.error("降级方法执行异常: {}", fallbackName, e);
            return unavailableResult;
        }
    }
}
