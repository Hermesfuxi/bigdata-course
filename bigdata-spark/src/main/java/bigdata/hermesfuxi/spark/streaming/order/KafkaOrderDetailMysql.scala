package bigdata.hermesfuxi.spark.streaming.order

import java.lang
import java.sql.{Connection, PreparedStatement}

import bigdata.hermesfuxi.spark.utils.{HBaseUtils, KafkaSparkUtils, MysqlUtils}
import com.alibaba.fastjson.JSON
import io.netty.util.internal.StringUtil
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/**
 * spark streaming 结合kafka 精确消费一次，并将结果保存到 mysql
 */
object KafkaOrderDetailMysql {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
    if (args.length > 0 && "true".equals(args(0))) {
      sparkConf.setMaster("local[*]")
    }

    val groupid = if(args.length < 2 || StringUtil.isNullOrEmpty(args(1)))  "g10002" else args(1)

    val topics = if(args.length < 3 || StringUtil.isNullOrEmpty(args(2)))  "topic" else args(2)

    val topicArr = topics.split(",")

    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")

    val streamingContext = new StreamingContext(sparkContext, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> KafkaSparkUtils.kafkaBrokerList,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null

    val offsetMap = KafkaSparkUtils.getOffsetMapFromMysql(groupid, topics)

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

    kafkaDStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        // 提交偏移量
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // mysql 提交偏移量有两种方案：Driver端提交、Executor端提交，提交量本身就是 Map[topicPartition] = offset，只要每个分区能找到各自对应的偏移量即可
        // KafkaWordCountMysql 已经实现了”Driver端提交偏移量“，这里实现”Executor端提交“
        rdd.map(item => {
          try {
            val orderBean: OrderBean = JSON.parseObject(item.value(), classOf[OrderBean])
            orderBean
          } catch {
            case ex: Exception => println(s"JSON解析错误，jsonStr：${item.value()}"); null
          }
        }).filter(_ != null)
          // foreachPartition后，在Executor端提交数据与偏移量
          .foreachPartition(iter => {
            var connection: Connection = null
            var ps: PreparedStatement = null
            try {
              connection = MysqlUtils.getConnection
              connection.setAutoCommit(false) // 关闭自动提交，开启事务

              //接收到的Kafk发送过来的数据为:ConsumerRecord(topic = spark_kafka, partition = 1, offset = 6, CreateTime = 1565400670211, checksum = 1551891492, serialized key size = -1, serialized value size = 43, key = null, value = hadoop spark ...)
              //注意:通过打印接收到的消息可以看到,里面有我们需要维护的offset,和要处理的数据
              //接下来可以对数据进行处理....或者使用transform返回和之前一样处理
              iter.foreach(orderBean => {
                ps = connection.prepareStatement("replace into spark_kafka_order (oid, cid, money, latitude, longitude) values(?, ?, ?, ?, ?)")
                ps.setString(1, orderBean.oid)
                ps.setLong(2, orderBean.cid)
                ps.setDouble(3, orderBean.money)
                ps.setDouble(4, orderBean.latitude)
                ps.setDouble(5, orderBean.longitude)
                ps.executeUpdate()
              })

              // 在Executor端获取到偏移量
              // 获取当前Task的PartitionID，然后到offsetRanges数组中取对应下标的偏移量，就是对应分区的偏移量
              val offsetRange = offsetRanges(TaskContext.getPartitionId())

              //处理数据的代码写完了,就该维护offset了,那么为了方便我们对offset的维护/管理,spark提供了一个类,帮我们封装offset的数据
              //获取当前批次RDD对应的偏移量（在Driver端获取到的）
              //实际中偏移量可以提交到MySQL/Redis中
              ps = connection.prepareStatement("replace into kafka_offset (`group_id`, `topic`, `partition`, `offset`) values(?, ?, ?, ?)")
              ps.setString(1, groupid)
              ps.setString(2, offsetRange.topic)
              ps.setInt(3, offsetRange.partition)
              ps.setLong(4, offsetRange.untilOffset)
              ps.executeUpdate()

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

          })
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
