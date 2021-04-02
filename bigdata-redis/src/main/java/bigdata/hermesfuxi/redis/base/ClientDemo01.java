package bigdata.hermesfuxi.redis.base;

import redis.clients.jedis.Jedis;

public class ClientDemo01 {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("hadoop-master", 6379);
        jedis.auth("123456");
//        String pingMessage = jedis.ping("hello world");

        jedis.set("name", "hello world");
        String name = jedis.get("name");
        System.out.println(name);
        jedis.close();
    }
}
