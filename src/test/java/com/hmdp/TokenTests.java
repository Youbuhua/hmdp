package com.hmdp;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class TokenTests {
    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private IUserService userService;

    @Test
    void test() {
        String filePath = "tokens.txt";
        String contentToWrite = "";
        int cnt = 0;
        for(int i = 1; i <= 1010; i ++) {
            User user = userService.getById(i);
            if(user != null) {
                cnt ++;
                // 将user信息存入到redis中 (token, userDTO) 过期时间
                String token = UUID.randomUUID().toString(true);
                String tokenKey = RedisConstants.LOGIN_USER_KEY + token;
                UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);

                // 将hashmap的字段值的类型全部转换未string类型 这样才能存入
                Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                        CopyOptions.create()
                                .setIgnoreNullValue(true)
                                .setFieldValueEditor((filedName, fileValue) -> fileValue.toString()));
                redisTemplate.opsForHash().putAll(tokenKey, userMap);
                redisTemplate.expire(tokenKey, RedisConstants.LOGIN_USER_TTL * 100, TimeUnit.MINUTES);
                contentToWrite = contentToWrite + "\n" + token;
            }
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            writer.write(contentToWrite);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }



}
