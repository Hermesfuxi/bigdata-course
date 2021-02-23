package bigdata.hermesfuxi.kafka.client;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerJavaDemo {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop-master:9092,hadoop-master2:9092,hadoop-slave1:9092,hadoop-slave2:9092,hadoop-slave3:9092");

        // ack应答:判别请求是否为完整的条件（判断是否发送成功） - “all”将会阻塞消息，这种设置性能最低，但是是最可靠的
        properties.put("acks", "all");

        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);

        properties.put("buffer.memory", 33554432);

        //指定 key 的序列化方式
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //指定 value 的序列化方式
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(properties);
        for(int i = 0; i < 100; i++){
            producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
        }

        producer.close();
    }
}
