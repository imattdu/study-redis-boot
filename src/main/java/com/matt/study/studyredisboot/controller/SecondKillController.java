package com.matt.study.studyredisboot.controller;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@RestController
public class SecondKillController {

    @Resource
    private RedisTemplate redisTemplate;

    // 乐观锁解决超卖问题
    @GetMapping("/sk")
    public String sk() {

        Random random = new Random();
        // [0,10]
        int i = random.nextInt(100000);
        String pid = "1";
        String uid = String.valueOf(i);

        String pk = "sk:p:" + pid;
        String uk = "sk:u";


        redisTemplate.execute(new SessionCallback<List<Object>>() {
            public List<Object> execute(RedisOperations operations) throws DataAccessException {
                operations.watch(pk);

                // 获取商品数量
                Integer pCnt = (Integer)(operations.opsForValue().get(pk));
                if (pCnt == null) {
                    System.out.println("还没开始");
                    return new ArrayList<>();
                }

                // 用户秒杀
                Boolean exists = operations.opsForSet().isMember(uk, uid);
                if (exists) {
                    System.out.println("不能重复下单");
                    return new ArrayList<>();
                }

                if (pCnt <= 0) {
                    System.out.println("商品不足");
                    return new ArrayList<>();
                }
                operations.multi();
                Long decrement = operations.opsForValue().decrement(pk);
                operations.opsForSet().add(uk, uid);
                return operations.exec();
            }
        });

        return uid;
    }

    // 乐观锁会有很多执行失败 从而导致库存遗留的问题
    @GetMapping("/sk1")
    public String sk1() {

        Random random = new Random();
        // [0,10]
        int i = random.nextInt(100000);
        String pid = "1";
        String uid = String.valueOf(i);

        String pk = "sk:p:" + pid;
        String uk = "sk:u";


        redisTemplate.execute(new SessionCallback<List<Object>>() {
            public List<Object> execute(RedisOperations operations) throws DataAccessException {
                operations.watch(pk);

                // 获取商品数量
                Integer pCnt = (Integer)(operations.opsForValue().get(pk));
                if (pCnt == null) {
                    System.out.println("还没开始");
                    return new ArrayList<>();
                }

                // 用户秒杀
                Boolean exists = operations.opsForSet().isMember(uk, uid);
                if (exists) {
                    System.out.println("不能重复下单");
                    return new ArrayList<>();
                }

                if (pCnt <= 0) {
                    System.out.println("商品不足");
                    return new ArrayList<>();
                }
                operations.multi();
                Long decrement = operations.opsForValue().decrement(pk);
                operations.opsForSet().add(uk, uid);
                return operations.exec();
            }
        });

        return uid;
    }

    /** 释放锁lua脚本 */
    private static final String RELEASE_LOCK_LUA_SCRIPT = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";

    @GetMapping("/lua")
    public void contextLoads() {
        String lockKey = "123";
        String UUID = "111";
        boolean success = redisTemplate.opsForValue().setIfAbsent(lockKey,UUID,3, TimeUnit.MINUTES);
        if (!success){
            System.out.println("锁已存在");
        }
        // 指定 lua 脚本，并且指定返回值类型
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(RELEASE_LOCK_LUA_SCRIPT,Long.class);
        // 参数一：redisScript，参数二：key列表，参数三：arg（可多个）
        Object execute = redisTemplate.execute(redisScript, Collections.singletonList(lockKey), UUID);
        System.out.println(execute);
    }

}
