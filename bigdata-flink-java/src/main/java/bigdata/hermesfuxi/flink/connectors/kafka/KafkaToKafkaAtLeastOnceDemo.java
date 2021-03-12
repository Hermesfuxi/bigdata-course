package bigdata.hermesfuxi.flink.connectors.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * 从 kafka 读取，再写入 kafka中，保持AtLeastOnce（至少一次）
 */
public class KafkaToKafkaAtLeastOnceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(60*1000, CheckpointingMode.AT_LEAST_ONCE);
        env.setStateBackend(new FsStateBackend("file:///D:/WorkSpaces/IdeaProjects/bigdata-course/.ck/flink"));

        Properties properties = new Properties();
        properties.load(KafkaToKafkaAtLeastOnceDemo.class.getClassLoader().getResourceAsStream("kafka.properties"));
        //设置消费者的事务隔离级别：只读已经提交事务的数据，脏数据不读
//        properties.setProperty("isolation.level", "read_committed");
        properties.setProperty("group.id", "KafkaToKafkaAtLeastOnceDemo");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>("kafka-in", new SimpleStringSchema(), properties);
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

        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                properties.getProperty("bootstrap.servers"),
                "kafka-out",
                new SimpleStringSchema()
        );
        resultStream.addSink(kafkaProducer);

        env.execute();
    }
}
