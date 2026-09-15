-- 释放锁：GET比对锁标识，一致才DEL —— "校验+删除"原子执行防止误删他人的锁
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
