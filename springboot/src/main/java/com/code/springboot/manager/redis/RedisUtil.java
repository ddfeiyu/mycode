package com.code.springboot.manager.redis;

import java.util.List;
import java.util.Map;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Map;


/**
 * redis大key删除方法
 */
public class RedisUtil {


        /**
         * 1. Hash删除: hscan + hdel
         * @param host
         * @param port
         * @param password
         * @param bigHashKey
         */
        public void delBigHash(String host, int port, String password, String bigHashKey) {
            Jedis jedis = new Jedis(host, port);
            if (password != null && !"".equals(password)) {
                jedis.auth(password);
            }
            ScanParams scanParams = new ScanParams().count(100);
            String cursor = "0";
            do {
                // HSCAN key cursor [MATCH pattern] [COUNT count]
                //迭代哈希表中的键值对。
                ScanResult<Map.Entry<String, String>> scanResult = jedis.hscan(bigHashKey, cursor, scanParams);
                List<Map.Entry<String, String>> entryList = scanResult.getResult();
                if (entryList != null && !entryList.isEmpty()) {
                    for (Map.Entry<String, String> entry : entryList) {
                        // HDEL key field1 [field2]
                        // 删除一个或多个哈希表字段
                        jedis.hdel(bigHashKey, entry.getKey());
                    }
                }
                cursor = scanResult.getStringCursor();
            } while (!"0".equals(cursor));
            //删除bigkey
            jedis.del(bigHashKey);
        }


        /**
         * 2. List删除: ltrim
         * @param host
         * @param port
         * @param password
         * @param bigListKey
         */
        public void delBigList(String host, int port, String password, String bigListKey) {
            Jedis jedis = new Jedis(host, port);
            if (password != null && !"".equals(password)) {
                jedis.auth(password);
            }
            // LLEN key
            // 获取列表长度
            long llen = jedis.llen(bigListKey);
            int counter = 0;
            int left = 100;
            while (counter < llen) {
                //每次从左侧截掉100个
                //  LTRIM key start stop
                //  对一个列表进行修剪(trim)，就是说，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除。
                jedis.ltrim(bigListKey, left, llen);
                counter += left;
            }
            //最终删除key
            jedis.del(bigListKey);
        }


        /**
         * 3. Set删除: sscan + srem
         * @param host
         * @param port
         * @param password
         * @param bigSetKey
         */
        public void delBigSet(String host, int port, String password, String bigSetKey) {
            Jedis jedis = new Jedis(host, port);
            if (password != null && !"".equals(password)) {
                jedis.auth(password);
            }
            ScanParams scanParams = new ScanParams().count(100);
            String cursor = "0";
            do {
                // SSCAN key cursor [MATCH pattern] [COUNT count]
                //迭代集合中的元素
                ScanResult<String> scanResult = jedis.sscan(bigSetKey, cursor, scanParams);
                List<String> memberList = scanResult.getResult();
                if (memberList != null && !memberList.isEmpty()) {
                    for (String member : memberList) {
                        //  SREM key member1 [member2]
                        //  移除集合中一个或多个成员
                        jedis.srem(bigSetKey, member);
                    }
                }
                cursor = scanResult.getStringCursor();
            } while (!"0".equals(cursor));
            //删除bigkey
            jedis.del(bigSetKey);
        }


        /**
         * 4. SortedSet删除: zscan + zrem
         * @param host
         * @param port
         * @param password
         * @param bigZsetKey
         */
        public void delBigZset(String host, int port, String password, String bigZsetKey) {
            Jedis jedis = new Jedis(host, port);
            if (password != null && !"".equals(password)) {
                jedis.auth(password);
            }
            ScanParams scanParams = new ScanParams().count(100);
            String cursor = "0";
            do {
                // ZSCAN key cursor [MATCH pattern] [COUNT count]
                //迭代有序集合中的元素（包括元素成员和元素分值）
                ScanResult<Tuple> scanResult = jedis.zscan(bigZsetKey, cursor, scanParams);
                List<Tuple> tupleList = scanResult.getResult();
                if (tupleList != null && !tupleList.isEmpty()) {
                    for (Tuple tuple : tupleList) {
                        //  ZREM key member [member ...]
                        //  移除有序集合中的一个或多个成员
                        jedis.zrem(bigZsetKey, tuple.getElement());
                    }
                }
                cursor = scanResult.getStringCursor();
            } while (!"0".equals(cursor));
            //删除bigkey
            jedis.del(bigZsetKey);
        }

    }

