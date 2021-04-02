package bigdata.hermesfuxi.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.*;

public class ConsumerDemo {
    public static void main(String[] args) {
        // 1 配置参数
        Properties props = new Properties();

        //从哪些broker消费数据
        props.setProperty("bootstrap.servers", "hadoop-master:9092,hadoop-master2:9092,hadoop-slave1:9092,hadoop-slave2:9092,hadoop-slave3:9092");
        // 反序列化的参数
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 指定group.id
        props.setProperty("group.id", "g001");

        // 指定消费的offset从哪里开始
        //earliest：从头开始 --from-beginning
        //latest:从消费者启动之后
        props.setProperty("auto.offset.reset", "earliest"); //[latest, earliest, none]

        // 是否自动提交偏移量  offset
        // enable.auto.commit 默认值就是true【5秒钟更新一次】，消费者定期会更新偏移量 groupid,topic,parition -> offset
        props.setProperty("enable.auto.commit", "false"); // 不让kafka自动维护偏移量     手动维护偏移量
        //enable.auto.commit   5000

        // 2 消费者的实例对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 订阅   参数类型  java的集合
        List<String> topics = Collections.singletonList("eagle-app-log");

        // 3 订阅主题
        consumer.subscribe(topics);

        while (true) {
            // 4  拉取数据
            ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(5000));
            for (ConsumerRecord<String, String> message : messages) {
                System.out.println(message);
                //获取该分区的最大的偏移量
                long offset = message.offset();
                System.out.println(offset);
            }
        }
    }
}
