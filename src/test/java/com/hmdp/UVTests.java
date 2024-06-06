package com.hmdp;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class UVTests {
    @Autowired
    private StringRedisTemplate redisTemplate;

    @Test
    void test() {

        String[] strings = new String[1000];

        int j = 0;
        for(int i = 0; i < 10000000; i ++) {
            j = i % 1000;
            strings[j] = "test_" + i;
            if(j == 999) {
                redisTemplate.opsForHyperLogLog().add("test_uv", strings);
            }
        }

        System.out.println(redisTemplate.opsForHyperLogLog().size("test_uv"));
    }



}
