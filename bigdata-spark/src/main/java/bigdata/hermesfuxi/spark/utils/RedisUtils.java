package bigdata.hermesfuxi.spark.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ResourceBundle;

/**
 * @author Hermesfuxi
 */
public class RedisUtils {
    public static JedisPool POOL;

    static {
        ResourceBundle resourceBundle = ResourceBundle.getBundle("redis-config");
        String host = resourceBundle.getString("host");
        int port = Integer.parseInt(resourceBundle.getString("port"));
        POOL = new JedisPool(host, port);
    }

    public static Jedis getRedisClient(){
        return POOL.getResource();
    }
}
