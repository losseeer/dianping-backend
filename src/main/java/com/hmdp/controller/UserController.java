package com.hmdp.controller;


import cn.hutool.core.bean.BeanUtil;
import com.hmdp.annotation.RateLimit;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.entity.UserInfo;
import com.hmdp.service.IUserInfoService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.*;

import com.hmdp.utils.RedisConstants;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

/**
 * <p>
 * 前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@RestController
@RequestMapping("/user")
public class UserController {

    @Resource
    private IUserService userService;

    @Resource
    private IUserInfoService userInfoService;

    @Autowired
    private StringRedisTemplate redisTemplate;

    /**
     * 发送手机验证码
     *
     * 【八股：验证码接口为什么需要限流？】
     * 1. 防止短信轰炸：恶意用户循环调用发验证码，消耗短信费用
     * 2. 一条短信0.04元，1秒发100条就是4元，一天就是34万
     *
     * 【为什么这一层不挂 @RateLimit 了？】
     * 原先这里是 @RateLimit(qps = 1) —— 全站每秒只允许 1 次发送。现在
     * @RateLimit 本身已经换成 Redis 分布式令牌桶（不再有单机失效的问题），
     * 但这里仍然不能用它，原因只剩一条：**维度不对**。
     *   1) 它限的是全站容量：2 个不同用户在同一秒就会互相冲突，5 个攻击者
     *      就能让全站用户收不到验证码
     *   2) 更关键的是它挡不住这个接口的真实攻击："用大量不同手机号轰炸"
     *      （短信泵送，按条计费）。对每个新号码都是首次发送，按全站 QPS 限
     *      只是把攻击者一起限住，防不住额度被逐个号码蚕食
     * 所以改为在 Service 层按手机号限流（SmsRateLimiter，Redis + Lua），
     * 三道闸门：同一号码 60 秒冷却 / 每日 10 条 / 全站兜底防枚举。
     * 放 Service 层还因为手机号格式校验在那里，能避免垃圾入参撑爆 Redis 键空间。
     *
     * 【八股：登录接口为什么也需要限流？】
     * 1. 防止暴力破解：攻击者不断尝试不同验证码/密码
     * 2. 防撞库：攻击者用泄露的账号密码批量尝试登录
     * 3. QPS=5：每秒只允许5次登录，正常用户足够，攻击者受限
     */
    @PostMapping("code")
    public Result sendCode(@RequestParam("phone") String phone) {
        return userService.sendCode(phone);
    }

    /**
     * 登录功能
     * @param loginForm 登录参数，包含手机号、验证码；或者手机号、密码
     */
    @PostMapping("/login")
    @RateLimit(qps = 5, message = "登录操作过于频繁，请稍后再试")
    public Result login(@RequestBody LoginFormDTO loginForm){
        return userService.login(loginForm);
    }

    /**
     * 登出功能
     * 清除 Redis 中的登录 Token 和 ThreadLocal 中的用户信息
     *
     * 【八股：登出为什么需要删除Redis中的Token？】
     * 1. 只清ThreadLocal没用：ThreadLocal是线程级别的，下次请求新线程里拿不到旧数据
     * 2. Token还在Redis中：任何人拿到这个Token还是可以冒充用户登录
     * 3. 安全要求：登出应该立即使Token失效，不能等服务端过期
     * 4. 拦截器逻辑：RefreshTokenInterceptor只要Token在Redis中有效就会放行
     *
     * @param request HTTP请求，用于获取header中的Token
     * @return 操作结果
     */
    @PostMapping("/logout")
    public Result logout(HttpServletRequest request){
        // 1. 从请求头获取 Token
        String token = request.getHeader("authorization");
        // 2. 清除 ThreadLocal 中的用户信息
        UserHolder.removeUser();
        // 3. 删除 Redis 中的登录 Token，使 Token 立即失效
        if (token != null && !token.isEmpty()) {
            String tokenKey = RedisConstants.LOGIN_USER_KEY + token;
            redisTemplate.delete(tokenKey);
            log.info("用户登出成功，Token已删除: {}", tokenKey);
        }
        return Result.ok();
    }

    @GetMapping("/me")
    public Result me(){
        UserDTO user = UserHolder.getUser();
        return Result.ok(user);
    }

    @GetMapping("/info/{id}")
    public Result info(@PathVariable("id") Long userId){
        // 查询详情
        UserInfo info = userInfoService.getById(userId);
        if (info == null) {
            // 没有详情，应该是第一次查看详情
            return Result.ok();
        }
        info.setCreateTime(null);
        info.setUpdateTime(null);
        // 返回
        return Result.ok(info);
    }

    @GetMapping("/{id}")
    public Result queryUserById(@PathVariable("id") Long userId){
        //查询详情
        User user = userService.getById(userId);
        if(user==null){
            return Result.ok();
        }
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        return Result.ok(userDTO);
    }

    /**
     * 签到功能
     * @return
     */
    @PostMapping("/sign")
    public Result sign(){
        return userService.sign();
    }

    /**
     * 统计连续签到
     * @return
     */
    @GetMapping("/sign/count")
    public Result signCount(){
        return userService.signCount();
    }
}
