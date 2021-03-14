package bigdata.hermesfuxi.spark.streaming.wordcnt

import bigdata.hermesfuxi.spark.utils.KafkaSparkUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("WARN")
    val streamingContext = new StreamingContext(sparkContext, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> KafkaSparkUtils.kafkaBrokerList,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "g1000",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean) // 消费者是否自动提交偏移量（Spark Task），为True时，重启后不会读取以前的消息
    )

    val topics = Array("wordcount")

    //从Kafka中读取数据创建DStream
    val kafkaDStream = KafkaUtils.createDirectStream(streamingContext,
      LocationStrategies.PreferConsistent, // 位置策略：就近原则
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams) // 消费策略：消息的topic和参数
    )

    //    kafkaDStream.map(_.value()).print()

    kafkaDStream.foreachRDD(rdd => {
      if (!rdd.isEmpty() && rdd.count() > 0) {
        //获取当前批次RDD对应的偏移量（在Driver端获取到的）
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (offsetRange <- offsetRanges) {
          println(s"topic: ${offsetRange.topic} , partition: ${offsetRange.partition} , offset: ${offsetRange.fromOffset} -> ${offsetRange.untilOffset}")
        }

        val value = rdd.flatMap(_.value().split("\\s+")).map((_, 1)).reduceByKey(_ + _)
        value.foreach(println)

        //        kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
