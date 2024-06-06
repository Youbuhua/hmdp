package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

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

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private CacheClient cacheClient;

    // 获取互斥锁 用于重建缓存
    private boolean tryLock(String lockKey) {
        Boolean b = redisTemplate.opsForValue().setIfAbsent(lockKey, "1", 10, TimeUnit.SECONDS);

        return BooleanUtil.isTrue(b);
    }

    // 删除互斥锁 用于重建缓存
    private void unLock(String lockKey) {
        redisTemplate.delete(lockKey);
    }

    // 缓存重建
    public void cacheRebuild(Long id, Long expireTime) throws InterruptedException {
        // 从数据库读到数据
        Shop tempShop = getById(id);

        // 模拟延迟
        Thread.sleep(200);

        // 重新写回redis中
        String shopKey = RedisConstants.CACHE_SHOP_KEY + id;
        RedisData tempData = new RedisData();
        tempData.setData(tempShop);
        tempData.setExpireTime(LocalDateTime.now().plusSeconds(expireTime));
        String jsonStr = JSONUtil.toJsonStr(tempData);
        redisTemplate.opsForValue().set(shopKey, jsonStr);

    }


    // 方法1 ----------------------------------
    // 使用 缓存空对象解决缓存穿透
    // 使用 设置随机过期时间解决缓存雪崩
    // 使用 互斥锁解决缓存击穿
//    @Override
//    public Result queryById(Long id) {
//        String shopKey = RedisConstants.CACHE_SHOP_KEY + id;
//
//        // 从redis中查询店铺缓存
//        String shopJson = redisTemplate.opsForValue().get(shopKey);
//
//        // 如果有缓存 则返回
//        // isBlank 是否为空 长度是否为0 是否包含空白字符
//        // isEmpty 是否为空 长度是否为0
//        // 若缓存的为空对象 也就是 shopJson = “” 符合isBlank 故不符合isNotBlank
//        if (StrUtil.isNotBlank(shopJson)) {
//            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
//            return Result.ok(shop);
//        }
//
//        // 判断是否是缓存的空对象
//        // 若缓存的为空对象 也就是 shopJson = “” 不为null
//        if(shopJson != null) {
//            return Result.fail("商家不存在");
//        }
//
//        // 缓存重建
//        // 获取互斥锁
//        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
//        Shop shop = null;
//        try {
//            // 未获取到互斥锁 则等待并重试
//            if(!tryLock(lockKey)) {
//                Thread.sleep(50);
//                return queryById(id);
//            }
//
//            // 获取到互斥锁
//            // 再次查询缓存是否存在数据
//            shopJson = redisTemplate.opsForValue().get(shopKey);
//            if (StrUtil.isNotBlank(shopJson)) {
//                shop = JSONUtil.toBean(shopJson, Shop.class);
//                return Result.ok(shop);
//            }
//            if(shopJson != null) {
//                return Result.fail("商家不存在");
//            }
//
//            // 若没有 则在数据库中查询
//            shop = getById(id);
//
//            // 模拟重建延时
//            Thread.sleep(200);
//
//            if(shop == null) {
//                // 为了解决缓存穿透 对在数据库未命中的请求 缓存空对象
//                redisTemplate.opsForValue().set(shopKey, "", RedisConstants.CACHE_NULL_TTL + RandomUtil.randomLong(0,3), TimeUnit.MINUTES);
//                return Result.fail("商家不存在");
//            }
//
//            // 将查询到的shop写入redis中去
//            String str = JSONUtil.toJsonStr(shop);
//            redisTemplate.opsForValue().set(shopKey, str, RedisConstants.CACHE_SHOP_TTL + RandomUtil.randomLong(0,10), TimeUnit.MINUTES);
//        }catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }finally {
//            unLock(lockKey);
//        }
//
//        return Result.ok(shop);
//    }

    // 方法2 ----------------------------------
    // 使用 逻辑过期时间解决缓存击穿（针对热点数据 缓存没有 直接返回）

    // 线程池
//    private static final ExecutorService CACHE_REBUILD_RXRCUTOR = Executors.newFixedThreadPool(10);
//    @Override
//    public Result queryById(Long id) {
//        String shopKey = RedisConstants.CACHE_SHOP_KEY + id;
//
//        // 从redis中查询店铺缓存
//        String shopJson = redisTemplate.opsForValue().get(shopKey);
//
//        // 如果没有缓存 则直接返回
//        // isBlank 是否为空 长度是否为0 是否包含空白字符
//        // isEmpty 是否为空 长度是否为0
//        if (StrUtil.isBlank(shopJson)) {
//            return Result.fail("商家不存在");
//        }
//
//        // 如果有缓存
//        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//        JSONObject data = (JSONObject) redisData.getData();
//        Shop shop = JSONUtil.toBean(data, Shop.class);
//
//        // 判断是否过期
//        if(expireTime.isAfter(LocalDateTime.now())) {
//            // 未过期 直接返回
//            return Result.ok(shop);
//        }
//
//        // 过期了 进行缓存重建 返回过期数据
//        // 获取互斥锁
//        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
//
//        // 获取到互斥锁
//        if(tryLock(lockKey)) {
//            // 再次查询缓存是否存在未过期数据
//            shopJson = redisTemplate.opsForValue().get(shopKey);
//            redisData = JSONUtil.toBean(shopJson, RedisData.class);
//            expireTime = redisData.getExpireTime();
//            data = (JSONObject) redisData.getData();
//            shop = JSONUtil.toBean(data, Shop.class);
//
//            if (expireTime.isAfter(LocalDateTime.now())) {
//                shop = JSONUtil.toBean(shopJson, Shop.class);
//                return Result.ok(shop);
//            }
//
//            // 若缓存还是没有，则开启新的线程 进行缓存重建
//            CACHE_REBUILD_RXRCUTOR.submit(() -> {
//                try {
//                    cacheRebuild(id, 20L);
//                }catch (Exception e) {
//                    throw new RuntimeException(e);
//                }finally {
//                    // 释放互斥锁
//                    unLock(lockKey);
//                }
//            });
//        }
//
//        // 返回过期数据
//        return Result.ok(shop);
//    }

// 方法3 ----------------------------------
// 使用封装的工具类解决缓存穿透
    @Override
    public Result queryById(Long id) {
        Shop shop = cacheClient.getWithPassThrough(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        if(shop == null) {
            Result.fail("商铺不存在");
        }

        return Result.ok(shop);
    }


// 方法4 ----------------------------------
// 使用封装的工具类解决缓存击穿
//    @Override
//    public Result queryById(Long id) {
//        Shop shop = cacheClient.getWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
//
//        if(shop == null) {
//            Result.fail("商铺不存在");
//        }
//
//        return Result.ok(shop);
//    }

    @Override
    public Result update(Shop shop) {
        Long id = shop.getId();

        if(id == null) {
            return Result.fail("id不能为空");
        }

        // 更新数据库
        updateById(shop);

        // 删缓存
        redisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + id);

        return null;
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 判断是否需要根据坐标查询
        if(x == null || y == null) {
            // 不需要坐标查询，按数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }

        // 要根据坐标进行查询
        // 分页开始 结束
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 取到前 end 项数据
        GeoResults<RedisGeoCommands.GeoLocation<String>> geoResults = redisTemplate.opsForGeo().search(RedisConstants.SHOP_GEO_KEY + typeId, GeoReference.fromCoordinate(x, y), new Distance(5000), RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end));
        if(geoResults == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = geoResults.getContent();

        // 截取 起到逻辑分页的作用 取到 from - end
        if(list.size() <= from) {
            return Result.ok(Collections.emptyList());
        }
        // 存储取到的全部id
        List<Long> ids = new ArrayList<>(list.size());
        // 保存id对应的距离
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });

        // 根据ids查询shopList
        String idStr = StrUtil.join(",", ids);

        List<Shop> shopList = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shopList) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }

        return Result.ok(shopList);
    }


}
