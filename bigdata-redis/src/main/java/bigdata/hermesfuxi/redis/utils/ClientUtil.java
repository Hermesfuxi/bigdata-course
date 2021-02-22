package bigdata.hermesfuxi.redis.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.ResourceBundle;

/**
 * @author Hermesfuxi
 */
public class ClientUtil {
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
