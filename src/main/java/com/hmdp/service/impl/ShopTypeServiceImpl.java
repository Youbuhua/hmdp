package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    StringRedisTemplate redisTemplate;

    @Override
    public Result queryTypeList() {

        // 从redis获取店铺类型的缓存
        String JSONStr = redisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_TYPE_KEY);

        if(StrUtil.isNotBlank(JSONStr)) {
            List<ShopType> typeList = JSONUtil.toList(JSONStr, ShopType.class);
            return Result.ok(typeList);
        }

        List<ShopType> typeList = query().orderByAsc("sort").list();

        if(typeList == null) {
            return Result.fail("查询商铺列表为空");
        }

        return Result.ok(typeList);
    }
}
