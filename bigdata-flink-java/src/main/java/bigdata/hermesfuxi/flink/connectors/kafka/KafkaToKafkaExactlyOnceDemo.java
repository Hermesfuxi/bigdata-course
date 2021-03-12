package bigdata.hermesfuxi.flink.connectors.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * 从 kafka 读取，再写入 kafka中，保持ExactlyOnce（精确一次）
 */
public class KafkaToKafkaExactlyOnceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(10*1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("file:///D:/WorkSpaces/IdeaProjects/bigdata-course/.ck/flink"));

        Properties sourceProps = new Properties();
        sourceProps.load(KafkaToKafkaExactlyOnceDemo.class.getClassLoader().getResourceAsStream("kafka.properties"));
        //设置消费者的事务隔离级别：只读已经提交事务的数据，脏数据不读
        sourceProps.setProperty("group.id", "KafkaToKafkaExactlyOnceDemo");
        sourceProps.setProperty("isolation.level", "read_committed");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>("kafka-in", new SimpleStringSchema(), sourceProps);
        // 在Checkpoint时不将偏移量写入到Kafka特殊的topic中
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);
        DataStreamSource<String> kafkaSource = env.addSource(kafkaConsumer);
        SingleOutputStreamOperator<String> upperStream = kafkaSource.map(String::toUpperCase);

        // 设计一个source，用于控制重启，以测量语义实现功能
        DataStreamSource<String> socketSource = env.socketTextStream("hadoop-slave3", 8888);
        SingleOutputStreamOperator<String> errorStream = socketSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if (value.contains("error")) {
                    throw new RuntimeException("数据出现异常！");
                }
                return value.toUpperCase();
            }
        });

        DataStream<String> resultStream = upperStream.union(errorStream);
        resultStream.print();

        Properties sinkProps = new Properties();
        sinkProps.setProperty("bootstrap.servers", sourceProps.getProperty("bootstrap.servers"));
        //允许事务最大的超时时间
        sinkProps.setProperty("transaction.timeout.ms",1000 * 60 * 5 + "");
        //使用的是AtLeastOnce语意（效率比Exactly-Once高）
        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>(
                "kafka-out",
                new KafkaStringSerializationSchema("kafka-out"),
                sinkProps,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
        resultStream.addSink(kafkaSink);
        // 注意：kafka的消费者一定要加上参数 --isolation-level read_committed
        // 不然依然会显示读取脏数据（默认 --isolation-level read_uncommitted）
        env.execute();
    }

    public static class KafkaStringSerializationSchema implements KafkaSerializationSchema<String> {
        private String topic;
        public KafkaStringSerializationSchema(String topic) {
            this.topic = topic;
        }
        @Override
        public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
            return new ProducerRecord<>(topic, element.getBytes(StandardCharsets.UTF_8));
        }
    }
}
