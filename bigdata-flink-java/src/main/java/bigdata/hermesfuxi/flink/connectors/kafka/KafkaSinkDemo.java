package bigdata.hermesfuxi.flink.connectors.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class KafkaSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("hadoop-slave3", 8888);

        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "hadoop-master:9092,hadoop-master2:9092,hadoop-slave1:9092,hadoop-slave2:9092,hadoop-slave3:9092",
                "wordcount",
                new SimpleStringSchema()
        );
        source.addSink(kafkaProducer);
        env.execute();
    }
}
