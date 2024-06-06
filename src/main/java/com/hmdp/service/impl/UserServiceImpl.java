package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
//        1、使用session处理
//        // 校验手机号码
//        if(RegexUtils.isPhoneInvalid(phone)) {
//            return Result.fail("请输入正确的手机号码");
//        }
//
//        // 生成验证码
//        String code = RandomUtil.randomNumbers(6);
//
//        // 保存到session
//        session.setAttribute("code", code);
//
//        // 发送验证码（用log替代）
//        log.debug("验证码：{}", code);
//
//        return Result.ok();


//      2、使用redis处理
        // 校验手机号码
        if(RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("请输入正确的手机号码");
        }

        // 生成验证码
        String code = RandomUtil.randomNumbers(6);

        // 保存到redis
        redisTemplate.opsForValue().set(RedisConstants.LOGIN_CODE_KEY + phone, code, RedisConstants.LOGIN_CODE_TTL, TimeUnit.MINUTES);

        // 发送验证码（用log替代）
        log.debug("验证码：{}", code);

        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
//        1、使用session处理
//        // 校验手机号码
//        if(RegexUtils.isPhoneInvalid(loginForm.getPhone())) {
//            return Result.fail("请输入正确的手机号码");
//        }
//
//        // 校验验证码
//        Object cacheCode = session.getAttribute("code");
//        String code = loginForm.getCode();
//        if(cacheCode == null || !cacheCode.equals(code)) {
//            return Result.fail("请输入正确的验证码");
//        }
//
//        // 查询当前手机号对应的用户是否注册
//        User user = query().eq("phone", loginForm.getPhone()).one();
//
//        // 未注册用户
//        if(user == null) {
//            user = createWithPhone(loginForm.getPhone());
//        }
//
//        // 保存用户信息(UserDto)到session中
//        session.setAttribute("user", BeanUtil.copyProperties(user, UserDTO.class));
//        return Result.ok();


//      2、使用redis处理
        // 校验手机号码
        String phone = loginForm.getPhone();
        if(RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("请输入正确的手机号码");
        }
        
        // 校验验证码
        String cachCode = redisTemplate.opsForValue().get(RedisConstants.LOGIN_CODE_KEY + phone);
        
        if(cachCode == null || !cachCode.equals(loginForm.getCode())) {
            return Result.fail("请输入正确的验证码");
        }
        
        // 查询当前手机号对应的用户是否注册
        User user = query().eq("phone", loginForm.getPhone()).one();

        // 未注册用户
        if(user == null) {
            user = createWithPhone(loginForm.getPhone());
        }
        
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
        redisTemplate.expire(tokenKey, RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);

        // 记录UV
        redisTemplate.opsForHyperLogLog().add(RedisConstants.UV_KEY, user.getId().toString());
        return Result.ok(token);

    }

    private User createWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));

        save(user);

        return user;
    }

    // 签到
    @Override
    public Result sign() {
        Long userId = UserHolder.getUser().getId();

        LocalDateTime now = LocalDateTime.now();
        String mon = now.format(DateTimeFormatter.ofPattern("yyyy:MM:"));
        int dayOfMonth = now.getDayOfMonth();

        // 往redis的BitMap中 存入当天的状态为 true 也就代表当天签过道了
        redisTemplate.opsForValue().setBit(RedisConstants.USER_SIGN_KEY + mon + userId, dayOfMonth - 1, true);

        return Result.ok("签到成功");
    }

    // 统计当月最长签到记录(当天必须签过到 否则返回0)
    @Override
    public Result countSignDay() {
        Long userId = UserHolder.getUser().getId();

        LocalDateTime now = LocalDateTime.now();
        String mon = now.format(DateTimeFormatter.ofPattern("yyyy:MM:"));
        int dayOfMonth = now.getDayOfMonth();

        // 取出这个月到今日的签到数据
        List<Long> signs = redisTemplate.opsForValue().bitField(RedisConstants.USER_SIGN_KEY + mon + userId, BitFieldSubCommands.create().get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0));
        if(signs == null || signs.isEmpty())  {
            return Result.ok(0);
        }

        Long sign = signs.get(0);
        if(sign == null || sign == 0) {
            return Result.ok(0);
        }

        int cnt = 0;
        while(true) {
            if((sign & 1) == 0) {
                break;
            }
            else {
                cnt ++;
            }
            // >>>无符号右移(高位填充零)
            // >> 有符号右移(用符号位(即原值最高位的值)填充新的高位)
            sign >>>= 1;
        }
        return Result.ok(cnt);
    }

    @Override
    public Result getUV() {
        return Result.ok(redisTemplate.opsForHyperLogLog().size(RedisConstants.UV_KEY));
    }
}
