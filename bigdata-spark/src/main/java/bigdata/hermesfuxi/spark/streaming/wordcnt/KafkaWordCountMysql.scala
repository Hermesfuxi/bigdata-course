package bigdata.hermesfuxi.spark.streaming.wordcnt

import java.sql.{Connection, PreparedStatement}

import bigdata.hermesfuxi.spark.utils.{KafkaSparkUtils, MysqlUtils}
import io.netty.util.internal.StringUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/**
 * Date 2021/02/23
 * Desc 使用Spark-Kafka-0-10版本整合,并手动提交偏移量,维护到MySQL中
 *
 * @author Hermesfuxi
 */
object KafkaWordCountMysql {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
    if (args.length > 0 && "true".equals(args(0))) {
      sparkConf.setMaster("local[*]")
    }

    val group_id = if(args.length< 2 || StringUtil.isNullOrEmpty(args(1)))  "g10001" else args(1)

    val topics = if(args.length< 3 || StringUtil.isNullOrEmpty(args(2)))  "topic" else args(2)
    val topicArr = topics.split(",")

    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")
    val streamingContext = new StreamingContext(sparkContext, Seconds(10))

    // 准备连接Kafka的参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> KafkaSparkUtils.kafkaBrokerList,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group_id,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean) // 消费者是否自动提交偏移量（Spark Task），为True时，重启后不会读取以前的消息
    )

    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null

    val offsetMap = KafkaSparkUtils.getOffsetMapFromMysql(group_id, topics)

    //从Kafka中读取数据创建DStream
    if (offsetMap != null) {
      //如果MySQL中没有记录offset,则直接连接,从latest开始消费
      kafkaDStream = KafkaUtils.createDirectStream(streamingContext,
        LocationStrategies.PreferConsistent, // 位置策略：就近原则
        ConsumerStrategies.Subscribe[String, String](topicArr, kafkaParams, offsetMap) // 消费策略：消息的topic和参数
      )
    } else {
      //如果MySQL中有记录offset,则应该从该offset处开始消费
      kafkaDStream = KafkaUtils.createDirectStream(streamingContext,
        LocationStrategies.PreferConsistent, // 位置策略：就近原则
        ConsumerStrategies.Subscribe[String, String](topicArr, kafkaParams) // 消费策略：消息的topic和参数
      )
    }

    // 手动维护偏移量: 消费了一小批数据就应该提交一次offset
    // 对DStream中的RDD进行操作: transform(转换) 和 foreachRDD(动作)
    kafkaDStream.foreachRDD(rdd => {
      // foreachRDD 是微批次（模拟流），每间隔一段时间，就会发送一个job，其数据集就是 rdd，可将此rdd当作 source 处理
      if (!rdd.isEmpty() && rdd.count() > 0) {
        var connection: Connection = null
        var ps: PreparedStatement = null
        try {
          connection = MysqlUtils.getConnection
          connection.setAutoCommit(false) // 关闭自动提交，开启事务

          //接收到的Kafk发送过来的数据为:ConsumerRecord(topic = spark_kafka, partition = 1, offset = 6, CreateTime = 1565400670211, checksum = 1551891492, serialized key size = -1, serialized value size = 43, key = null, value = hadoop spark ...)
          //注意:通过打印接收到的消息可以看到,里面有我们需要维护的offset,和要处理的数据
          //接下来可以对数据进行处理....或者使用transform返回和之前一样处理
          val result = rdd.flatMap(_.value().split("\\s+")).map((_, 1)).reduceByKey(_ + _)
          // 考虑到事务控制，需要将数据收集到Driver端提交（避免分布式事务难题，微批次的数据量不会过大）
          for (wordResult <- result.collect()) {
            // insert ... on duplicate key update: 键相同时更新，键不同时插入，在需要旧值时使用
            ps = connection.prepareStatement("insert into kafka_wc (word, cnt) values(?, ?)  ON DUPLICATE KEY UPDATE cnt=cnt+?")
            ps.setString(1, wordResult._1)
            ps.setLong(2, wordResult._2)
            ps.setLong(3, wordResult._2)
            ps.executeUpdate()
          }

          //处理数据的代码写完了,就该维护offset了,那么为了方便我们对offset的维护/管理,spark提供了一个类,帮我们封装offset的数据
          //获取当前批次RDD对应的偏移量（在Driver端获取到的）
          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

          //实际中偏移量可以提交到MySQL/Redis中
          for (offsetRange <- offsetRanges) {
            //replace into： 键相同时整行替换，键不同时插入，在不需要旧值时使用（先删除后插入）
            ps = connection.prepareStatement("replace into kafka_offset (`group_id`, `topic`, `partition`, `offset`) values(?, ?, ?, ?)")
            ps.setString(1, group_id)
            ps.setString(2, offsetRange.topic)
            ps.setInt(3, offsetRange.partition)
            ps.setLong(4, offsetRange.untilOffset)
            ps.executeUpdate()
          }
          connection.commit()
        } catch {
          case exception: Exception => {
            connection.rollback()
            exception.printStackTrace()
          }
        } finally {
          if (ps != null) {
            ps.close()
          }
          if (connection != null) {
            connection.close()
          }
        }
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
