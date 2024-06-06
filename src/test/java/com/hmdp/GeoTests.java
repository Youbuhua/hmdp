package com.hmdp;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Shop;
import com.hmdp.entity.User;
import com.hmdp.service.IShopService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SpringBootTest
class GeoTests {
    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private IShopService shopService;

    @Test
    void test() {
        List<Shop> shopList = shopService.list();

        Map<Long, List<Shop>> collect = shopList.stream().collect(Collectors.groupingBy(Shop::getTypeId));

        for (Map.Entry<Long, List<Shop>> entry : collect.entrySet()) {
            Long typeId = entry.getKey();

            List<Shop> shops = entry.getValue();

//            for(Shop shop : shops) {
//                redisTemplate.opsForGeo().add(RedisConstants.SHOP_GEO_KEY + typeId, new Point(shop.getX(), shop.getY()), shop.getId().toString());
//            }

            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(shops.size());

            for(Shop shop : shops) {
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),
                        new Point(shop.getX(), shop.getY())
                ));

                redisTemplate.opsForGeo().add(RedisConstants.SHOP_GEO_KEY + typeId, locations);
            }


        }



    }



}
