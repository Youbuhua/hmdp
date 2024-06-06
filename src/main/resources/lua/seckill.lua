-- 优惠券id
local voucherId = ARGV[1]
-- 用户id
local userId = ARGV[2]
-- 订单id
local orderId = ARGV[3]

-- 库存key
local stockKey = 'seckill:stock:' .. voucherId
-- 订单key
local orderKey = 'seckill:order:' .. voucherId

-- 判断库存
if(tonumber(redis.call('get', stockKey)) < 1) then
    -- 库存不足 返回 1
    return 1
end

-- 判断是否下单
if(redis.call('sismember', orderKey, userId) == 1) then
    -- 重复下单 返回 2
    return 2
end

-- 可以下单
-- 扣库存
redis.call('incrby', stockKey, -1)
-- 添加 useId 到 orderKey 的 set 中
redis.call('sadd', orderKey, userId)
-- 发送消息到队列中
redis.call('xadd', 'stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)
-- 返回 0
return 0