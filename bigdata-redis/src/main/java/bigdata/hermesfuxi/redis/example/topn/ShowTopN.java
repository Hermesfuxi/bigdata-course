package bigdata.hermesfuxi.redis.example.topn;

import bigdata.hermesfuxi.redis.utils.ClientUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.Set;

public class ShowTopN {
    public static void main(String[] args) throws InterruptedException {
        Jedis redisClient = ClientUtil.getRedisClient();
        while (true){
            Set<Tuple> tupleSet = redisClient.zrangeWithScores("tasks", 0, -1);
            System.out.println(tupleSet);
            Thread.sleep(200);
        }
    }
}
