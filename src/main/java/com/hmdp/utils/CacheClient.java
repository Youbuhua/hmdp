package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@Component
public class CacheClient {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_RXRCUTOR = Executors.newFixedThreadPool(10);

    // 方法: 将任意Java对象序列化为json并存储在string类型的key中，并且可以设置TTL过期时间
    // 存入 设置TTL过期时间
    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    // 方法: 将任意Java对象序列化为json并存储在string类型的key中，并且可以设置逻辑过期时间，用于处理缓存击穿问题
    // 存入 设置逻辑过期时间
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));

        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }


    // 方法: 根据指定的key查询缓存，并反序列化为指定类型，利用存储空值的方式解决缓存穿透问题
    // 获取 利用缓存空值解决缓存穿透问题(穿透：请求无效数据 大量无效请求直接来到数据库)
    public <R, ID> R getWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        String jsonStr = stringRedisTemplate.opsForValue().get(key);

        if(StrUtil.isNotBlank(jsonStr)) {
            return JSONUtil.toBean(jsonStr, type);
        }

        if(jsonStr != null) {
            return null;
        }

        R result = dbFallback.apply(id);

        if(result == null) {
            this.set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }

        this.set(key, result, time, unit);
        return result;
    }


    // 方法: 根据指定的key查询缓存，并反序列化为指定类型，利用逻辑过期解决缓存击穿问题
    // 获取 利用逻辑过期时间解决缓存击穿问题(击穿：热点数据的key无效 大量请求直接来到数据库)
    public <R, ID> R getWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        String jsonStr = stringRedisTemplate.opsForValue().get(key);

        if(StrUtil.isBlank(jsonStr)) {
            return null;
        }

        RedisData redisData = JSONUtil.toBean(jsonStr, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        LocalDateTime expireTime = redisData.getExpireTime();
        R result = JSONUtil.toBean(data, type);

        if(expireTime.isAfter(LocalDateTime.now())) {
            return result;
        }

        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;

        if(tryLock(lockKey)) {
            jsonStr = stringRedisTemplate.opsForValue().get(key);
            redisData = JSONUtil.toBean(jsonStr, RedisData.class);
            data = (JSONObject) redisData.getData();
            expireTime = redisData.getExpireTime();
            result = JSONUtil.toBean(data, type);

            if(expireTime.isAfter(LocalDateTime.now())) {
                unLock(lockKey);
                return result;
            }

            CACHE_REBUILD_RXRCUTOR.submit(() -> {
                try {
                    R tempResult = dbFallback.apply(id);
                    RedisData tempRedisData = new RedisData();
                    tempRedisData.setData(tempResult);
                    tempRedisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));

                    stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(tempRedisData));
//                    this.setWithLogicalExpire(key, JSONUtil.toJsonStr(tempRedisData), -1L, TimeUnit.SECONDS);
                }catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    unLock(lockKey);
                }
            });

        }

        return result;
    }

    // 获取互斥锁 用于重建缓存
    private boolean tryLock(String lockKey) {
        Boolean b = stringRedisTemplate.opsForValue().setIfAbsent(lockKey, "1", 10, TimeUnit.SECONDS);

        return BooleanUtil.isTrue(b);
    }

    // 删除互斥锁 用于重建缓存
    private void unLock(String lockKey) {
        stringRedisTemplate.delete(lockKey);
    }
}
