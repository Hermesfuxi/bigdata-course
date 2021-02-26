package bigdata.hermesfuxi.kafka.client

import java.time.Duration
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer


//topic、分区、groupId -> 偏移量
object ConsumerScalaDemo {

  def main(args: Array[String]): Unit = {

    // 1 配置参数
    val props = new Properties()

    //从哪些broker消费数据
    props.setProperty("bootstrap.servers", "hadoop-master:9092,hadoop-master2:9092,hadoop-slave1:9092,hadoop-slave2:9092,hadoop-slave3:9092")
    // 反序列化的参数
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    // 指定group.id
    props.setProperty("group.id", "g005")

    // 指定消费的offset从哪里开始
    //earliest：从头开始 --from-beginning
    //latest:从消费者启动之后
    props.setProperty("auto.offset.reset", "earliest") //[latest, earliest, none]

    // 是否自动提交偏移量  offset
    // enable.auto.commit 默认值就是true【5秒钟更新一次】，消费者定期会更新偏移量 groupid,topic,parition -> offset
    props.setProperty("enable.auto.commit", "false") // 不让kafka自动维护偏移量     手动维护偏移量
    //enable.auto.commit   5000

    // 2 消费者的实例对象
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)

    // 订阅   参数类型  java的集合
    val topic: util.List[String] = java.util.Arrays.asList("test")

    // 3 订阅主题
    consumer.subscribe(topic)

    while (true) {
      // 4  拉取数据
      val msgs: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(5000))

      //导入隐式转换
      import scala.collection.JavaConverters._
      //将Java的集合或迭代器转成Scala的集合或迭代器
      for (cr <- msgs.asScala) {
        //ConsumerRecord[String, String]
        println(cr)

        //msgs迭代器
        //if(msgs.isEmpty) {
        //获取该分区的最大的偏移量
        //}
      }
    }

    //consumer.close()

  }
}
