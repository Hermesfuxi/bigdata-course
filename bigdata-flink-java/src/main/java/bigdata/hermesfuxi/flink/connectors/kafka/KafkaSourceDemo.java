package bigdata.hermesfuxi.flink.connectors.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        int parallelismSize = environment.getParallelism();
        System.out.println("程序默认的并行度为：" + parallelismSize);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop-master:9092,hadoop-master2:9092,hadoop-slave1:9092,hadoop-slave2:9092,hadoop-slave3:9092");
        properties.setProperty("group.id", "group01");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>("wordcount", new SimpleStringSchema(), properties);

        DataStreamSource<String> kafkaDataStreamSource = environment.addSource(kafkaConsumer);
        kafkaDataStreamSource.print();

        int kafkaSourceParallelismSize = kafkaDataStreamSource.getParallelism();
        System.out.println("kafka source 的并行度为：" + kafkaSourceParallelismSize);

        environment.execute();
    }
}
