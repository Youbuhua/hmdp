package com.hmdp.interceptor;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.UserDTO;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;


@Component

public class RefreshTokenInterceptor implements HandlerInterceptor {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
//        1、使用session处理
//        // 获取session
//        HttpSession session = request.getSession();

//        // 获取session中存的user
//        UserDTO user = (UserDTO) session.getAttribute("user");
//        if(user == null) {
//            response.setStatus(401);
//            return false;
//        }

//        // 将用户信息保存在ThreadLocal这
//        UserHolder.saveUser(user);

//        // 放行
//        return true;


//      2、使用redis处理
        // 获取token
        String token = request.getHeader("authorization");
        String tokenKey = RedisConstants.LOGIN_USER_KEY + token;

        // 基于token获取user
        Map<Object, Object> userMap = redisTemplate.opsForHash().entries(tokenKey);
        if(userMap.isEmpty()) {
            return true;
        }

        // 将userMap转换未UserDTO对象
        UserDTO user = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);

        // 将用户信息保存在ThreadLocal这
        UserHolder.saveUser(user);

        // 刷新token有效期
        redisTemplate.expire(tokenKey, RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);

        // 放行
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        // 在请求结束后 移除用户
        UserHolder.removeUser();
    }
}
