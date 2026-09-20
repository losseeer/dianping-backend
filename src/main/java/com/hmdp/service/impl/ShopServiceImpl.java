package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.listener.TransactionOutboxPublisher;
import com.hmdp.listener.TransactionOutboxWriter;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Resource
    private TransactionOutboxWriter outboxWriter;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Override
    public Result queryById(Long id){
        // 先用逻辑过期策略查缓存（热点key场景）
        Shop shop = cacheClient.
                queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        // 逻辑过期策略前提是缓存已预热；如果缓存未命中（非热点/首次访问/Redis未启动），
        // 再走缓存穿透策略兜底，该策略会主动查DB并回填缓存，方便联调。
        if (shop == null) {
            shop = cacheClient
                    .queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        }
        if(shop==null){
            return Result.fail("店铺不存在！");
        }
        return Result.ok(shop);

    }

    // 【八股：缓存预热】逻辑过期方案的前提：热点key必须在流量到来前主动写入Redis，
    // 否则第一次访问必然miss，防击穿无从谈起（当前项目配合 queryById 的穿透兜底形成双保险）

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id==null){
            return Result.fail("店铺id不能为空");
        }
        // 【八股：Cache Aside 写路径——先更新DB，再删除缓存】
        // 为什么删缓存而不是更新缓存：并发写下两边计算结果可能乱序互相覆盖；
        // 删除=懒加载，下次读miss时自然回填最新值
        // 为什么先库后缓存：反过来(先删缓存后更库)的窗口里，读请求会把旧值回填进缓存，
        // 脏数据要活到下一次删除；先库后缓存最坏只是短暂旧数据，删除失败还能补偿重删
        // 加强方案：延迟双删（更新后延迟几百ms再删一次，覆盖回填窗口），本项目短TTL下可省略
        //1.先修改数据库
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY+shop.getId());
        //3.投 ES 增量同步事件（与 updateById 同事务：库改成功 = 事件一定在）
        submitEsSync(id);
        return Result.ok();
    }

    /**
     * 新增商铺 —— 覆写 MyBatis-Plus 的 save，是为了把「落库」和「投 ES 同步事件」绑在同一个事务里。
     *
     * 原来 ShopController.saveShop 只调 shopService.save()，新商铺从此在索引里查不到；
     * 靠调用方「记得顺手多调一次同步接口」是迟早会漂移的写法，也是一种双写（写库成功、写 ES 失败
     * 的中间态回滚不了）。放到这里，任何走 IShopService#save 的写入都自动挂上同步事件，
     * 忘记同步不再是可能的错误。
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean save(Shop shop) {
        if (!super.save(shop)) {
            return false;
        }
        submitEsSync(shop.getId());
        return true;
    }

    /**
     * 在当前业务事务内投一条 ES 同步事件，由 TransactionOutboxPublisher 异步回查 MySQL 后写索引。
     *
     * 【为什么 eventKey 要额外拼一个全局唯一 id】
     * tb_transaction_outbox 上有 uk_event_key 唯一索引，而 TransactionOutboxWriter 撞重复键时
     * 只打一行 debug 就当无事发生。如果这里用 "es-sync:{shopId}" 这种纯业务键去做幂等，
     * 同一个商铺的第二次更新会被静默吞掉 —— 只有第一次改动能同步到 ES，并且不报任何错。
     * 所以刻意让每次写入都产生新键：同步的正确性不依赖事件去重，而依赖消费端
     * 「只取 id、回查 MySQL 当前值」（见 IShopSearchService#syncShopById），
     * 重复投递只是把同一份数据再写一遍，天然幂等。
     *
     * 【为什么只传 id 不传实体】updateById 只更新非空字段，传进来的 Shop 常常只有半条数据，
     * 拿它拼 ES 文档会把没提交的字段写成 null。
     */
    private void submitEsSync(Long shopId) {
        outboxWriter.save("es-sync:" + shopId + ":" + redisIdWorker.nextId("outbox-es"),
                TransactionOutboxPublisher.ES_SYNC, shopId,
                Collections.<String, Object>singletonMap("shopId", shopId));
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //1.是否根据坐标查询
        if(x==null||y==null){
            //不需要坐标查询，该数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }
        //2.计算分页参数
        int from=(current-1)*SystemConstants.DEFAULT_PAGE_SIZE;
        int end=current*SystemConstants.DEFAULT_PAGE_SIZE;

        //3.查询redis，按照距离排序，分页。结果：shopId,distance
        // 【八股：GEO 附近的店】底层是 GeoHash：经纬度交错编码成一维字符串，
        // 前缀相同=地理相邻，把"圆形范围检索"转化为有序集合上的前缀范围查询
        // 按 typeId 分 key（shop:geo:{typeId}）避免单一大key；member存shopId，距离由Redis计算
        String key = SHOP_GEO_KEY + typeId;
        //在 Redis 中按地理坐标（x, y）查询距离当前用户位置 5000 米内的商店
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        if(results==null){
            return Result.ok(Collections.emptyList());
        }
        //4.解析出id
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if(list.size()<=from){
            return Result.ok(Collections.emptyList());
        }
        //4.1.截取从from到end部分  跳过前 from 个结果，实现分页
        List<Long> ids=new ArrayList<>(list.size());
        Map<String,Distance> distanceMap=new HashMap<>(list.size());
        list.stream().skip(from).forEach(result->{
            //4.2.获取店铺id
            String shopIdStr=result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            //4.3.获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr,distance);
        });
        //5.根据id查询shop
        // 【八股：order by field(id,...) 保序】in 查询返回是主键序而非距离序，
        // 用 field() 按传入顺序重排；注意这是拼接SQL，id必须只来自服务端（防SQL注入）
        // 已知局限：GEO limit(end) 取回前N页再内存skip，深分页会多取数据，改进方向是游标式分页
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query()
                .in("id", ids).last("order by field(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        //6.返回
        return Result.ok(shops);
    }
}
