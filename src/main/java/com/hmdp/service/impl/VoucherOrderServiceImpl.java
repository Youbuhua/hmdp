package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Autowired
    private RedisIdWorker redisIdWorker;

    @Autowired
    private ISeckillVoucherService seckillVoucherService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private RedissonClient redissonClient;

//-------------------------------同步下单： 数据库判断库存+订单操作 redis只负责限制一人一单的锁
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        // 判断优惠券信息
//        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
//        if(seckillVoucher == null) {
//            return Result.fail("优惠券不存在");
//        }
//        if(seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀未开始");
//        }
//        if(seckillVoucher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀已结束");
//        }
//        if(seckillVoucher.getStock() < 1) {
//            return Result.fail("库存不足");
//        }
//
//        Long userId = UserHolder.getUser().getId();
//
////        方法1：使用synchronized锁
////        synchronized (userId.toString().intern()) {
////            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
////            return proxy.createVoucherOrder(voucherId, userId);
////        }
//
//
//////        方法2：使用分布式redis锁(自定义)
////        SimpleRedisLock redisLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
////
////        if(!redisLock.tryLock(1200)) {
////            // 未获取到锁
////            return Result.fail("同一个用户不能同时下单");
////        }
////
////        try {
////            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
////            return proxy.createVoucherOrder(voucherId, userId);
////        }catch (Exception e) {
////            throw new RuntimeException(e);
////        }finally {
////            // 释放锁
////            redisLock.unlock();
////        }
//
//
////      方法3：使用分布式redis锁(Redisson)
//        RLock redissonClientLock = redissonClient.getLock("lock:order:" + userId);
//
//        if(!redissonClientLock.tryLock()) {
//            // 未获取到锁
//            return Result.fail("同一个用户不能同时下单");
//        }
//
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId, userId);
//        }catch (Exception e) {
//            throw new RuntimeException(e);
//        }finally {
//            // 释放锁
//            redissonClientLock.unlock();
//        }
//
//
//    }
//
//    @Transactional
//    public Result createVoucherOrder(Long voucherId, Long userId) {
//
//        // 判断是否当前用户已购买当前优惠券(一人一单)
//        int count = query().eq("user_id", userId)
//                .eq("voucher_id", voucherId)
//                .count();
//        if(count > 0) {
//            // 用户已购买
//            return Result.fail("用户已购买过了");
//        }
//
//        // 扣减库存
//        boolean success = seckillVoucherService
//                .update()
//                .setSql("stock = stock - 1")
//                .eq("voucher_id", voucherId)
//                .gt("stock", 0)
//                .update();
//        if(!success) {
//            return Result.fail("库存不足");
//        }
//
//        // 生成订单
//        long orderId = redisIdWorker.nextId("order");
//        VoucherOrder voucherOrder = new VoucherOrder();
//        voucherOrder.setId(orderId);
//        voucherOrder.setVoucherId(voucherId);
//        voucherOrder.setUserId(userId);
//
//        save(voucherOrder);
//
//        return Result.ok(orderId);
//    }



//------------------------------- 异步下单：redis处理能否下订单
    private IVoucherOrderService proxy;

    @Autowired
    private StringRedisTemplate redisTemplate;
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("/lua/seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";
        @Override
        public void run() {
            while(true) {
                try {
                    // 获取消息队列中的订单信息 XGROUNP GROUP g1 c1 COUNT 1 BLOCK 2000 STEAMS stream.orders >
                    List<MapRecord<String, Object, Object>> list = redisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );

                    // 获取失败 没有消息 继续下一次循环
                    if(list == null || list.isEmpty()) {
                        continue;
                    }

                    // 获取成功 有消息 可以下单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    handleVoucherOrder(voucherOrder);
                    // ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());

                }catch (Exception e) {
                    // 出现异常 则在pengList中获取未确认的消息 进而再次处理
                    log.error("处理订单异常", e);
                    try {
                        handlePendingList();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }

        }

        private void handlePendingList() throws InterruptedException {
            while(true) {
                try {
                    // 获取pendList列中的订单信息 XGROUNP GROUP g1 c1 COUNT 1 BLOCK 2000 STEAMS stream.orders
                    List<MapRecord<String, Object, Object>> list = redisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );

                    // 获取失败 没有消息 代表pendList没有异常消息 结束循环
                    if(list == null || list.isEmpty()) {
                        break;
                    }

                    // 获取成功 有消息 可以下单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    handleVoucherOrder(voucherOrder);
                    // ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());

                }catch (Exception e) {
                    // 出现异常 则在pengList中获取未确认的消息 进而再次处理
                    log.error("处理pendList订单异常", e);
                    Thread.sleep(20);
                }
            }
        }
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {

        // 获取用户
        Long userId = voucherOrder.getUserId();

        // 并发 一个用户一把锁(Redisson 分布式锁)
        // 创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);

        // 获取锁
        if(!lock.tryLock()) {
            // 获取锁失败
            log.error("不允许重复下单");
            return;
        }

        try {
            // 调用代理对象 进而执行事务(createVoucherOrder)
            // 这样确保了 先拿到锁 再有spring代理的对象执行事务 提交事务后 再释放锁
            proxy.createVoucherOrder(voucherOrder);
        }catch (Exception e) {
            throw new RuntimeException(e);
        }finally {
            // 释放锁
            lock.unlock();
        }
    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 判断是否当前用户已购买当前优惠券(一人一单)
        int count = query().eq("user_id", voucherOrder.getUserId())
                .eq("voucher_id", voucherOrder.getVoucherId())
                .count();

        if(count > 0) {
            // 用户已购买
            log.error("用户已购买了一次");
            return;
        }

        // 第一次对数据库的表的操作
        // 扣减库存
        boolean success = seckillVoucherService
                .update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();
        if(!success) {
            log.error("库存不足");
            return;
        }

        // 第二次对数据库的表的操作
        // 写入订单
        save(voucherOrder);

    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long orderId = redisIdWorker.nextId("order");
        // 执行lua脚本
        // lua脚本职责
        // (1) 判断库存 解决超卖
        // (2) 处理一人一单
        // (3) 更新 redis 存入的数据 voucher:stock voucher:order
        // (4) 向 redis 的 stream.orders 发送消息
        Long res = redisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                UserHolder.getUser().getId().toString(),
                orderId.toString());

        if(res.intValue() == 1) {
            return Result.fail("库存不足");
        }

        if(res.intValue() == 2) {
            return Result.fail("一人只能买一单");
        }

        // 获取代理对象 (将代理对象直接保存为这个类的成员属性 便于子线程拿到这个代理对象)
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        return Result.ok(orderId);
    }




}
