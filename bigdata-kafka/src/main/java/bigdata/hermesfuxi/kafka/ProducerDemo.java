package bigdata.hermesfuxi.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        // 1 配置参数
        Properties props = new Properties();
        // 连接kafka节点
        props.put("bootstrap.servers", "hadoop-master:9092,hadoop-master2:9092,hadoop-slave1:9092,hadoop-slave2:9092,hadoop-slave3:9092");

        // ack应答:判别请求是否为完整的条件（判断是否发送成功） - “all”将会阻塞消息，这种设置性能最低，但是是最可靠的
        props.put("acks", "all");

        // 如果请求失败，生产者会自动重试，我们指定是0次，如果启用重试，则会有重复消息的可能性。
        props.put("retries", 0);

        // producer(生产者)缓存每个分区未发送的消息。缓存的大小是通过 batch.size 配置指定的。
        // 值较大的话将会产生更大的批，并需要更多的内存（因为每个“活跃”的分区都有1个缓冲区）
        props.put("batch.size", 1024 * 10);

        // 逗留时间：默认逗留时间为0，即缓冲可立即发送，即便缓冲空间还没有满，但是，如果你想减少请求的数量，可以设置linger.ms大于0。
        // 这将指示生产者发送请求之前等待一段时间，希望更多的消息填补到未满的批中。这类似于TCP的算法
        // 例如可能100条消息在一个请求发送，因为我们设置了linger(逗留)时间为1毫秒，然后，如果我们没有填满缓冲区，这个设置将增加1毫秒的延迟请求以等待更多的消息。
        // 需要注意的是，在高负载下，相近的时间一般也会组成批，即使是 linger.ms=0。在不处于高负载的情况下，如果设置比0大，以少量的延迟代价换取更少的，更有效的请求。
        props.put("linger.ms", 1);

        // buffer.memory 控制生产者可用的缓存总量，如果消息发送速度比其传输到服务器的快，将会耗尽这个缓存空间。
        // 当缓存空间耗尽，其他发送调用将被阻塞，阻塞时间的阈值通过max.block.ms设定，之后它将抛出一个TimeoutException。
        props.put("buffer.memory", 1024 * 1024 * 3);

        //指定 key 的序列化方式
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //指定 value 的序列化方式
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 将用户提供的key和value对象ProducerRecord转换成字节，你可以使用附带的ByteArraySerializaer或StringSerializer处理简单的string或byte类型


        // 根据已有分区
        String topic = "test"; //三个分区{0, 1, 2}

        // 2 kafka的生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 1011; i < 1020; i++) {
            // 3 封装的对象

            //将数据发送到指定的分区编号
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, 2, null, "value:" + i);

            //val partitionNum = i % 3  // 指定数据均匀写入3个分区中
            //val record = new ProducerRecord[String, String](topic, partitionNum, null,"myvalue:"+i)


            //不指定分区编号，指定key，   分区编号 = key.hasacode % 3
            //相同key的数据一定会到kafka的同一个分区，但是同一个分区中可以有多个key的数据
            //val record = new ProducerRecord[String, String](topic , "sdfasdf","myvalue:"+i)


            //根据key的hashcode值模除以topic分区的数量，返回一个分区编号
            //val record = new ProducerRecord[String, String](topic , UUID.randomUUID().toString ,"myvalue:"+i)

            //没有指定Key和分区，默认的策略就是轮询，将数据均匀写入多个分区中
            //val record = new ProducerRecord[String, String](topic,"value-" + i)

            // 4 发送消息
            producer.send(record);

        }

        System.out.println("message send success");

        //producer.flush()
        // 释放资源
        producer.close();
    }
}
