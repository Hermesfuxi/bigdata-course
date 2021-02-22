package bigdata.hermesfuxi.kafka.client

import java.util.{Properties, UUID}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object ProducerDemo {

  def main(args: Array[String]): Unit = {

    // 1 配置参数
    val props = new Properties()
    // 连接kafka节点
    props.setProperty("bootstrap.servers", "node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092")
    //指定key序列化方式
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //指定value序列化方式
    props.setProperty("value.serializer", classOf[StringSerializer].getName) // 两种写法都行

    //props.setProperty("acks", "1") //ack应答
    //props.setProperty("compression.type", "gzip") //压缩方式
    //props.setProperty("retries", "100") //重试次数
    //props.setProperty("request.timeout.ms", "60000") //超时时间

    val topic = "doit" //三个分区{0, 1, 2}

    // 2 kafka的生产者
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
    for (i <- 1011 to 1020) {
      // 3 封装的对象

      //将数据发送到指定的分区编号
      val record = new ProducerRecord[String, String](topic, 2 , null, "myvalue:"+i)

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
      producer.send(record)

    }

    println("message send success")

    //producer.flush()
    // 释放资源
    producer.close()
  }
}
