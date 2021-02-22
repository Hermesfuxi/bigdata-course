package bigdata.hermesfuxi.redis.example.topn;

import bigdata.hermesfuxi.redis.utils.ClientUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.Random;
import java.util.Set;

/**
 * 数组
 * 随机获取
 * 对应的 +1值
 */
public class Player {
    public static void main(String[] args) throws InterruptedException {
        Jedis redisClient = ClientUtil.getRedisClient();
        redisClient.flushAll();
        String[] tasks = new String[]{"娜娜", "慧慧", "奔奔", "小康", "baby", "小明", "涛哥"};
        for (String task : tasks) {
            redisClient.zadd("tasks", 1, task);
        }
//        Set<Tuple> tupleSet = redisClient.zrangeWithScores("tasks", 0, -1);
//        System.out.println(tupleSet);
        Random random = new Random();
        while (true) {
            int index = random.nextInt(tasks.length);
            String task = tasks[index];
            System.out.println(task + "任务队列....");

            redisClient.zincrby("tasks", 1, task);
            Double taskScore = redisClient.zscore("tasks", task);

            System.out.println(task + "第" + taskScore + "被执行了");

            Thread.sleep(200);
        }
    }
}
