package bigdata.hermesfuxi.flink.connectors.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaToRedisAtLeastOnceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(30000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new RocksDBStateBackend("file:///D:/WorkSpaces/IdeaProjects/bigdata-course/.ck/flink"));

        Properties sourceProps = new Properties();
        sourceProps.load(KafkaToKafkaExactlyOnceDemo.class.getClassLoader().getResourceAsStream("kafka.properties"));
        //设置消费者的事务隔离级别：只读已经提交事务的数据，脏数据不读
        sourceProps.setProperty("group.id", "KafkaToRedisAtLeastOnceDemo");
        sourceProps.setProperty("isolation.level", "read_committed");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>("word-count", new SimpleStringSchema(), sourceProps);
        // 在Checkpoint时不将偏移量写入到Kafka特殊的topic中
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);
        DataStreamSource<String> kafkaSource = env.addSource(kafkaConsumer);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = kafkaSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split("\\s+");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(t -> t.f0).sum(0);

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(16379).setPassword("123456").build();
        result.addSink(new RedisSink(conf, new RedisMapper<Tuple2<String, Integer>>() {

            //写入Redis的方法，value使用HASH类型，并指定外面key的值得名称（可理解为 redis 表名）
            //WORD_COUNT -> {(spark,5), (flink,6)}
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "WORD_COUNT");
            }

            @Override
            public String getKeyFromData(Tuple2<String, Integer> tuple2) {
                return tuple2.f0;
            }

            @Override
            public String getValueFromData(Tuple2<String, Integer> tuple2) {
                return tuple2.f1.toString();
            }
        }));

        env.execute();
    }
}
