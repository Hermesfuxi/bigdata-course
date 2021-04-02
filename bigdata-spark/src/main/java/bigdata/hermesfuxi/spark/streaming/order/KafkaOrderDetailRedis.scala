package bigdata.hermesfuxi.spark.streaming.order

import java.lang

import bigdata.hermesfuxi.spark.utils.{HBaseUtils, KafkaSparkUtils, RedisUtils}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
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
import redis.clients.jedis.{Jedis, Pipeline}

/**
 * spark streaming 结合kafka 精确消费一次，并将结果保存到 redis
 */
object KafkaOrderDetailRedis {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
    if (args.length > 0 && "true".equals(args(0))) {
      sparkConf.setMaster("local[*]")
    }else{
      sparkConf.setMaster("yarn-cluster")
    }

    val groupid = if (args.length < 2 || StringUtil.isNullOrEmpty(args(1))) "g10002" else args(1)

    val topics = if (args.length < 3 || StringUtil.isNullOrEmpty(args(2))) "topic" else args(2)

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

    val offsetMap = KafkaSparkUtils.getOffsetMapFromRedis(groupid, topics)

    //从Kafka中读取数据创建DStream
    if (offsetMap != null) {
      //如果 redis 中没有记录offset,则直接连接,从latest开始消费
      kafkaDStream = KafkaUtils.createDirectStream(streamingContext,
        LocationStrategies.PreferConsistent, // 位置策略：就近原则
        ConsumerStrategies.Subscribe[String, String](topicArr, kafkaParams, offsetMap) // 消费策略：消息的topic和参数
      )
    } else {
      //如果 redis 中有记录offset,则应该从该offset处开始消费
      kafkaDStream = KafkaUtils.createDirectStream(streamingContext,
        LocationStrategies.PreferConsistent, // 位置策略：就近原则
        ConsumerStrategies.Subscribe[String, String](topicArr, kafkaParams) // 消费策略：消息的topic和参数
      )
    }

    kafkaDStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        // 提交偏移量
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.map(item => {
          try {
            val orderBean: OrderBean = JSON.parseObject(item.value(), classOf[OrderBean])
            orderBean
          } catch {
            case ex: Exception => println(s"JSON解析错误，jsonStr：${item.value()}"); null
          }
        })
          .filter(_ != null)
          .foreachPartition(iter => {
            var redisClient: Jedis = null
            var pipeline: Pipeline = null
            try{
              redisClient = RedisUtils.getRedisClient
              // 开始事务
              pipeline = redisClient.pipelined()
              pipeline.multi()

              iter.foreach(orderBean => {
                //将计算好的结果写入到Redis中
                pipeline.hset("spark_kafka_order", orderBean.oid, JSON.toJSONString(orderBean, SerializerFeature.WriteMapNullValue));
              })
              // 在Executor端获取到偏移量
              // 获取当前Task的PartitionID，然后到offsetRanges数组中取对应下标的偏移量，就是对应分区的偏移量
              val offsetRange = offsetRanges(TaskContext.getPartitionId())
              val topic = offsetRange.topic
              val partition = offsetRange.partition
              val offset = offsetRange.untilOffset
              pipeline.hset("kafka_offset" + "_" + groupid, topic + "_" + partition, String.valueOf(offset));
              // 提交事务
              pipeline.exec()
              pipeline.sync()

            } catch {
              case exception: Exception => {
                exception.printStackTrace()
                pipeline.discard()
                // 停掉application
                streamingContext.stop(true)
              }
            } finally {
              if (pipeline != null) {
                pipeline.close()
              }
              if (redisClient != null) {
                redisClient.close()
              }
            }
          })
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
