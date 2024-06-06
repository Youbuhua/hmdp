package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;

import cn.hutool.core.lang.UUID;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{

    private StringRedisTemplate redisTemplate;

    private String keyName;

    public SimpleRedisLock(String keyName, StringRedisTemplate redisTemplate) {
        this.keyName = keyName;
        this.redisTemplate = redisTemplate;
    }

    private static final String KEY_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) + '-';

    @Override
    public boolean tryLock(long timeoutSec) {
        // 获取当前线程ID
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        // 获取锁
        Boolean success = redisTemplate.opsForValue().setIfAbsent(KEY_PREFIX + keyName, threadId, timeoutSec, TimeUnit.SECONDS);

        return BooleanUtil.isTrue(success);
    }

    // 方法1 java的版本
//    @Override
//    public void unlock() {
//        // 释放锁
//        String threadId = ID_PREFIX + Thread.currentThread().getId();
//
//        // 判断是否是自己的锁
//        if(threadId.equals(redisTemplate.opsForValue().get(KEY_PREFIX + keyName))) {
//            redisTemplate.delete(KEY_PREFIX + keyName);
//        }
//
//    }

    // 方法2 采用lua脚本实现

    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("/lua/unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    @Override
    public void unlock() {
        // 调用lua脚本 执行判断 + 删除
        redisTemplate.execute(UNLOCK_SCRIPT,
                Collections.singletonList(KEY_PREFIX + keyName),
                ID_PREFIX + Thread.currentThread().getId());
    }
}
