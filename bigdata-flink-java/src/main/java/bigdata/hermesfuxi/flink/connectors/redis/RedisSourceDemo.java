package bigdata.hermesfuxi.flink.connectors.redis;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Hermesfuxi
 * redis source: 使用jedis api（flink-connector-redis限制太多，体验不好）
 */
public class RedisSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<HashMap<String, Integer>> source = env.addSource(new RedisSourceFunction());
        source.print();

        env.execute();
    }

    private static class RedisSourceFunction extends RichSourceFunction<HashMap<String,Integer>> {
        private final Logger logger = LoggerFactory.getLogger(RedisSourceFunction.class);

        private Jedis jedis = null;
        private Boolean isRunning = true;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("localhost", 16379);
            jedis.auth("123456");
        }

        @Override
        public void run(SourceContext<HashMap<String,Integer>> ctx) throws Exception {
            HashMap<String, Integer> kvMap = new HashMap<String, Integer>();
            int restartCount = 3;
            while (isRunning && restartCount > 0) {
                try {
                    kvMap.clear();
                    Map<String, String> wordCountMap = jedis.hgetAll("WORD_COUNT");
                    for (Map.Entry<String, String> entry : wordCountMap.entrySet()) {
                        String key = entry.getKey();
                        String value = entry.getValue();
                        kvMap.put(key, Integer.valueOf(value));
                    }
                    if (kvMap.size() > 0) {
                        ctx.collect(kvMap);
                        cancel();
                    } else {
                        logger.warn("从redis中获取的数据为空");
                    }
                } catch (JedisConnectionException e) {
                    logger.warn("redis连接异常，需要重新连接", e.getCause());
                    open(null);
                    restartCount --;
                } catch (Exception e) {
                    logger.warn(" source 数据源异常", e.getCause());
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
            while (jedis != null) {
                jedis.close();
            }
        }
    }
}
