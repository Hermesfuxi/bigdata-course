package bigdata.hermesfuxi.flink.connectors.kafka;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop-master:9092,hadoop-master2:9092,hadoop-slave1:9092,hadoop-slave2:9092,hadoop-slave3:9092");
        properties.setProperty("group.id", "group01");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

//        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>("wordcount", new SimpleStringSchema(), properties);

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>("word-count", new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String nextElement) {
                return StringUtils.isEmpty(nextElement);
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                String topic = record.topic();
                int partition = record.partition();
                long offset = record.offset();
                return topic + "-" + partition + "-" + offset;
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return TypeInformation.of(String.class);
            }
        }, properties);

        DataStreamSource<String> kafkaDataStreamSource = environment.addSource(kafkaConsumer);

        kafkaDataStreamSource.print();

        environment.execute();
    }
}
