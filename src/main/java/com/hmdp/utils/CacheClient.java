package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * 缓存工具类 —— 【八股：缓存三大问题（穿透/雪崩/击穿）解决方案】
 *
 * 面试高频：缓存的三个经典问题一定要能区分清楚！
 *  - 缓存穿透：查不存在的数据 → 缓存空值 / 布隆过滤器
 *  - 缓存雪崩：大量key同时失效 → 随机过期时间 / 缓存预热
 *  - 缓存击穿：热点key失效 → 互斥锁 / 逻辑过期
 *
 * 【八股：Cache Aside 模式（旁路缓存）】
 * 本项目采用的是 Cache Aside 模式：
 *  - 读：先读缓存，缓存没有再读数据库，然后写入缓存
 *  - 写：先更新数据库，再删除缓存（注意是删除不是更新）
 * 为什么是删除缓存而不是更新缓存？
 *  - 更新缓存可能导致并发写时数据不一致
 *  - 删除缓存更简单，下次读的时候自然会从数据库加载最新数据
 *  - 懒加载思想：用到了才去查，不用就不加载，节省内存
 */
@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;
    private final MeterRegistry meterRegistry;


    public CacheClient(StringRedisTemplate stringRedisTemplate, MeterRegistry meterRegistry) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.meterRegistry = meterRegistry;
        // 重建线程池的队列深度/活跃线程/被丢弃任务数由 Micrometer 现成的采集器提供。
        // 这里必须采：队列打满时走的是 DiscardPolicy（只有一行 warn 日志），
        // 表现为"缓存一直是旧的、stale_served 一直下不来"，却没有任何请求报错。
        ExecutorServiceMetrics.monitor(meterRegistry, CACHE_REBUILD_EXECUTOR, "cache-rebuild");
    }

    /**
     * 缓存查询结果计数。
     *
     * 【为什么要自己埋而不是看 Redis 的 keyspace_hits/misses】后者是整个实例的全局
     * 计数，把秒杀库存、Token、限流桶和商铺缓存混在一起，算不出"商铺详情命中率"
     * 这种业务口径；而且它只统计 GET 命中的 key，分不清"命中了防穿透的空值"。
     *
     * @param cache 缓存命名空间（即 keyPrefix，如 shop:id:），基数等于接入的缓存种类数
     */
    private void countLookup(String cache, String result) {
        meterRegistry.counter("dianping.cache.lookup", "cache", cache, "result", result).increment();
    }

    /** 重建尝试计数：claimed=本次抢到锁 / skipped=锁被占 / failed=重建任务抛异常 */
    private void countRebuild(String cache, String outcome) {
        meterRegistry.counter("dianping.cache.rebuild", "cache", cache, "outcome", outcome).increment();
    }


    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    /**
     * 设置逻辑过期时间 —— 【八股：逻辑过期 vs 物理过期】
     *
     * 物理过期：用Redis的EXPIRE命令，到时间Redis自动删除key
     * - 优点：简单，Redis帮你处理
     * - 缺点：热点key过期的瞬间，大量请求打到数据库（缓存击穿）
     *
     * 逻辑过期：value里存一个过期时间字段，Redis本身永不过期
     * - 优点：热点key不会真的过期，用户永远能拿到数据（可能旧一点）
     * - 缺点：占用内存，需要额外线程重建缓存，可能短暂脏读
     *
     * 适用场景：
     * - 物理过期：普通数据，对一致性要求不那么高
     * - 逻辑过期：热点数据，对可用性要求极高，允许短暂不一致
     */
    public void setWithLogicalExpire(String key,Object value,Long time,TimeUnit unit){
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));
    }

    /**
     * 带缓存穿透防护的查询 —— 【八股：什么是缓存穿透？】
     *
     * 定义：用户请求的数据在缓存和数据库中都不存在
     * 危害：每次请求都会打到数据库，恶意用户可以用不存在的id打爆数据库
     *
     * 解决方案对比：
     * 1. 缓存空值（本项目采用）
     *    - 优点：实现简单
     *    - 缺点：占用内存，可能缓存了大量无用的空key
     *    - 优化：空值设置较短的过期时间（如2分钟）
     *
     * 2. 布隆过滤器(Bloom Filter)
     *    - 原理：位图+多个哈希函数，判断"一定不存在"或"可能存在"
     *    - 优点：内存占用极小
     *    - 缺点：有误判率（说存在的可能不存在，说不存在的一定不存在）
     *    - 适用：数据量极大，查询量极高的场景
     *
     * 3. 接口参数校验
     *    - 比如id必须是正整数，非法请求直接拦截
     *    - 这是第一道防线，成本最低
     *
     * 【八股：为什么空值要用较短的过期时间？】
     * 因为空值没有业务价值，只是为了防穿透
     * 如果设置太长，万一之后数据库真的加了这条数据，缓存里还是空的，用户看不到
     * 设置短一点（比如2分钟），既能防穿透，又能较快更新
     */
    public <R,ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,Long time,TimeUnit unit){
        String key=keyPrefix+id;
        //1.尝试从Redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断缓存是否存在
        if(StrUtil.isNotBlank(json)) { //判断字符串既不为null，也不是空字符串(""),且也不是空白字符
            //3.存在，返回商铺信息
            countLookup(keyPrefix, "hit");
            return JSONUtil.toBean(json, type);

        }
        //判断是否为空值 —— 【八股：空值判断的细节】
        // json不为null但内容是空字符串，说明这是我们缓存的空值（防穿透用的）
        // 直接返回null，不再查数据库，这就是缓存穿透防护
        // 【指标口径】null_hit 单独成一档：它不查 DB，算命中；和 miss_db_absent
        // 混在一起的话，"缓存挡掉了多少穿透请求"就再也算不出来了。
        if(json!=null){
            countLookup(keyPrefix, "null_hit");
            return null;
        }
        //4.不存在，根据id查询数据库
        R r = dbFallback.apply(id);
        //5.判断数据库中是否存在
        if(r==null){
            //6.不存在，返回错误状态码
            // 【八股：缓存空值防穿透】
            // 数据库也不存在，就缓存一个空字符串，设置较短过期时间
            // 下次再来查同样的id，直接从缓存拿到空值，不会打到数据库
            stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
            countLookup(keyPrefix, "miss_db_absent");
            return null;
        }
        //7.存在，写入redis，返回商铺信息
       this.set(key,r,time,unit);
        countLookup(keyPrefix, "miss_db_found");

        return r;

    }


    /**
     * 缓存重建线程池 —— 【八股：为什么禁用Executors.newFixedThreadPool？】
     *
     * newFixedThreadPool 的三个隐患（阿里Java规约禁用Executors的原因）：
     * 1. 队列是无界 LinkedBlockingQueue：DB故障时重建任务无限堆积 → OOM
     * 2. 默认线程非daemon且无关闭钩子：Spring容器关闭后JVM退不出去
     * 3. submit()的异常被封进被丢弃的Future，静默消失
     *
     * 本池参数对着"缓存重建"业务校准：
     * - core=2/max=8：重建是低频异步任务，keepAlive 60s 回收空闲线程
     * - 有界队列32：DB慢时最多积压几十个任务，超出走拒绝策略而非膨胀
     * - 拒绝策略Discard（记日志）：重建任务可丢——丢了不损数据，下一个请求
     *   发现key仍逻辑过期会重新触发（天然幂等重试）；CallerRuns反而错：
     *   会让用户请求线程执行DB重建，违背逻辑过期"用户永不等待"的初衷
     */
    private final ThreadPoolExecutor CACHE_REBUILD_EXECUTOR = new ThreadPoolExecutor(
            2, 8, 60, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(32),
            r -> {
                Thread t = new Thread(r, "cache-rebuild");
                t.setDaemon(true);
                t.setUncaughtExceptionHandler((thread, e) -> log.error("缓存重建线程未捕获异常", e));
                return t;
            },
            new ThreadPoolExecutor.DiscardPolicy() {
                @Override
                public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                    log.warn("缓存重建队列已满，任务被丢弃，等待下次请求重新触发");
                    super.rejectedExecution(r, executor);
                }
            });

    /** 锁TTL：需覆盖最坏情况下慢DB查询+写回的耗时；过短则锁提前过期放行重复重建 */
    private static final long LOCK_TTL_SECONDS = 30;

    /** 释放锁脚本：GET比对锁标识一致才DEL，原子防误删（与seckill.lua同款加载方式） */
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    @PreDestroy
    public void shutdownRebuildExecutor() {
        CACHE_REBUILD_EXECUTOR.shutdown();
    }

    /**
     * 逻辑过期方式解决缓存击穿 —— 【八股：什么是缓存击穿？】
     *
     * 定义：某一个热点key，在过期的瞬间，有大量并发请求
     * 这些请求发现缓存没了，同时去查数据库，数据库瞬间压力暴增
     *
     * 和缓存穿透的区别：
     * - 穿透：数据不存在
     * - 击穿：数据存在，但缓存过期了，而且是热点数据
     *
     * 和缓存雪崩的区别：
     * - 雪崩：大量key同时失效
     * - 击穿：单个热点key失效
     *
     * 解决方案对比：
     * 1. 互斥锁（mutex lock）
     *    - 原理：缓存失效时，只有一个线程能获取锁去查数据库重建缓存
     *    - 优点：一致性高，实现相对简单
     *    - 缺点：其他线程需要等待，有性能损耗，可能死锁
     *
     * 2. 逻辑过期（本方法采用）
     *    - 原理：缓存永不过期，value里存过期时间，过期了开后台线程重建
     *    - 优点：用户永远不会等（直接返回旧数据），性能好
     *    - 缺点：数据不一致（返回旧数据），占用内存，实现复杂
     *
     * 适用场景：
     * - 互斥锁：对数据一致性要求高，能接受短暂等待
     * - 逻辑过期：对可用性要求极高，能接受短暂不一致（如商品详情、排行榜）
     *
     * 【八股：为什么用线程池而不是直接new Thread？】
     * - 线程池可以控制并发数，防止线程过多耗尽资源
     * - 线程复用，减少创建销毁线程的开销
     * - 池参数设计（有界队列/拒绝策略/守护线程）见 CACHE_REBUILD_EXECUTOR 字段注释
     */
    public <R,ID> R queryWithLogicalExpire(
            String keyPrefix,ID id,Class<R> type,Function<ID,R> dbFallback,Long time,TimeUnit unit){
        String key=keyPrefix+id;
        //1.尝试从Redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断缓存是否存在
        if(StrUtil.isBlank(json)) { //isBlank：为null、空串("")或纯空白字符时为true（与isNotBlank相反）
            //3.缓存不存在，直接返回null
            // 【八股：逻辑过期方案的前提】
            // 逻辑过期方案假设热点key已经预热到缓存中了（key无物理TTL，理论上常驻）
            // 如果缓存里根本没有，说明不是热点数据，走queryWithPassThrough兜底
            // （真实项目中应该配合缓存预热机制，把热点数据提前加载）
            // 【指标口径】这一档在监控上是要盯的异常信号而非常规命中路径：
            // 它说明预热没做到位，请求正落回 queryWithPassThrough 打 DB。
            countLookup(keyPrefix, "not_cached");
            return null;

        }

        //4.存在，将json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        // 【两种缓存策略共用一个 key，值格式必须都认得】
        // queryWithPassThrough 回填的是"裸 JSON + 物理 TTL"（见 set()），而本方法假定读到的
        // 是逻辑过期信封 RedisData。ShopServiceImpl.queryById 先走本方法、拿不到再走穿透兜底，
        // 同一个 key 的两种写法就会互相撞上：以前照 RedisData 取 expireTime 去比时间，
        // 撞上裸 JSON 直接 NPE，被熔断器降级成一次 DB 查询 —— 客户端依然 200，所以谁也没发现。
        // 裸 JSON 本身就是刚回填、带物理 TTL 的有效值，认下来直接返回，还省掉后面那趟回源。
        // 指标上单独记 hit_plain：这个数不为零就说明两种策略仍在共用 key，是可收敛的信号。
        if (redisData == null || redisData.getExpireTime() == null
                || !(redisData.getData() instanceof JSONObject)) {
            R plain = JSONUtil.toBean(json, type);
            countLookup(keyPrefix, "hit_plain");
            return plain;
        }
        R shop = JSONUtil.toBean((JSONObject) redisData.getData(),type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())) {
            //5.1.未过期，直接返回店铺信息
            countLookup(keyPrefix, "hit");
            return shop;
        }
        //5.2.已过期，需要返回缓存重建
        //6.缓存重建
        //6.1.获取互斥锁（锁value用UUID标识持有者，释放时校验防止误删他人的锁）
        String lockKey=RedisConstants.LOCK_SHOP_KEY+id;
        String lockValue=UUID.randomUUID().toString();
        boolean isLock = tryLock(lockKey, lockValue);
        // 【指标口径】stale_served = 用户拿到了逻辑已过期的旧数据。这是逻辑过期方案
        // 唯一的"不一致"证据，量级突然放大说明 DB 或重建线程出了问题。
        countLookup(keyPrefix, "stale_served");
        //6.2.判断是否获取锁成功
        if(isLock){
            //  6.3.成功，提交异步重建任务
            // 【八股：Double Check（双重检查）】
            // 任务开头再读一次缓存：等锁/排队的间隙，可能别的线程已经重建完了
            // （发现未过期就直接返回，省一次DB查询）
            countRebuild(keyPrefix, "claimed");
            CACHE_REBUILD_EXECUTOR.execute(()->{
                try {
                    String freshJson = stringRedisTemplate.opsForValue().get(key);
                    if (StrUtil.isNotBlank(freshJson)) {
                        RedisData fresh = JSONUtil.toBean(freshJson, RedisData.class);
                        if (fresh.getExpireTime() != null && fresh.getExpireTime().isAfter(LocalDateTime.now())) {
                            return;
                        }
                    }
                   //查询数据库
                    R r1= dbFallback.apply(id);
                    //写入redis
                    this.setWithLogicalExpire(key,r1,time,unit);
                } catch (Exception e) {
                    // 异步任务的异常必须当场落日志：execute()不似submit()会把异常封进Future
                    // 静默吞掉——重建失败靠"下一个请求重新触发"自愈，但必须留痕可查
                    // 【指标口径】留痕现在也要可查：靠日志数不出"这一分钟重建失败了几次"，
                    // 而反复失败会让 stale_served 一直涨，两个指标对着看才定位得准。
                    countRebuild(keyPrefix, "failed");
                    log.error("缓存重建失败 key={}", key, e);
                }finally {
                    //释放锁：必须传lockKey。历史bug曾误传数据key，导致
                    // 1) 锁key只能等TTL兜底释放，期间其他重建线程抢不到锁
                    // 2) 缓存数据key被误删，逻辑过期防击穿失效
                    unLock(lockKey, lockValue);
                }
            });

        } else {
            // 锁被别的请求持有，本次不重建（正常现象：热点key过期瞬间只有一个赢家）
            countRebuild(keyPrefix, "skipped");
        }

        //6.4.返回过期的商铺信息
        // 【八股：逻辑过期的核心思想】
        // 不管缓存过没过期，都先返回数据给用户
        // 如果过期了，后台偷偷重建，用户无感知
        // 这就是"空间换时间"，用一点内存和短暂不一致，换极高的可用性
        return shop;

    }
    /**
     * 创建锁 —— 【八股：基于setnx的简易分布式锁实现】
     *
     * setIfAbsent = SETNX（SET if Not eXists）
     * 只有key不存在时才能设置成功，利用这个特性实现互斥
     *
     * 【八股：为什么要设置过期时间？】
     * 如果不设置过期时间，万一获取锁的服务宕机了，锁永远不会释放
     * 其他服务永远拿不到锁，造成死锁
     * 过期时间是兜底机制，哪怕服务挂了，锁也能自动释放
     *
     * 【八股：setIfAbsent + 过期时间 必须是原子操作】
     * 不能先set再expire，因为如果set之后expire之前服务宕机了
     * 锁就没有过期时间，变成死锁
     * Redis 2.6.12之后SET命令支持NX+EX参数，可以一步完成
     * Spring的setIfAbsent(key, value, timeout, unit)就是封装了这个命令
     *
     * 【八股：锁value为什么存UUID而不是"1"？】
     * 释放锁时要校验"锁是不是自己加的"，value必须能标识持有者
     * 本场景锁由请求线程获取、由池内重建线程释放，不能用线程ID标识
     *
     * @param key
     * @param lockValue 锁持有者标识（UUID），释放时校验防误删
     * @return
     */
    private boolean tryLock(String key, String lockValue){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, lockValue, LOCK_TTL_SECONDS, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放锁 —— 【八股：为什么要"校验+删除"原子化？】
     *
     * 裸DEL的误删场景：
     * 1. 线程A获取锁，设置10秒过期
     * 2. 线程A业务执行了15秒（超过了过期时间）
     * 3. 第10秒时，锁自动过期释放了
     * 4. 线程B此时获取到了锁
     * 5. 线程A业务执行完了，执行unLock删除锁
     * 6. 线程A把线程B的锁给删了！
     *
     * 本实现：锁value存UUID标识持有者，unlock.lua里GET比对一致才DEL。
     * "判断+删除"必须在Lua里原子执行：分开写的话，比对通过后锁恰好过期、
     * 别人拿到锁，后续DEL还是删了别人的
     * （生产级实现见 Redisson：可重入Hash结构 + 看门狗续期，本项目秒杀已采用）
     */
    private void unLock(String key, String lockValue){
        stringRedisTemplate.execute(UNLOCK_SCRIPT, Collections.singletonList(key), lockValue);
    }
}
