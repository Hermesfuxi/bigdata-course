package bigdata.hermesfuxi.spark.streaming.order

import java.lang

import bigdata.hermesfuxi.spark.utils.{HBaseUtils, KafkaSparkUtils}
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
 * phoenix:  建表语句：
 * CREATE TABLE "doit.spark_kafka_order"
 * (RK VARCHAR PRIMARY KEY,
 * "orders"."cid" UNSIGNED_INT,
 * "orders"."money" UNSIGNED_DOUBLE,
 * "orders"."longitude" UNSIGNED_DOUBLE,
 * "orders"."latitude" UNSIGNED_DOUBLE,
 * "offsets"."groupid" VARCHAR,
 * "offsets"."topic" VARCHAR,
 * "offsets"."partition" UNSIGNED_INT,
 * "offsets"."offset" UNSIGNED_LONG)
 * COLUMN_ENCODED_BYTES='NONE';
 *
 * 查询语句：
 * select "topic", "partition", max("offset") as "offset" from "spark_kafka_order" where "groupid" = ? group by "topic", "partition"
 *
 */
object KafkaOrderDetailHbase {
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

    val offsetMap = KafkaSparkUtils.getOffsetMapFromHBase(groupid, topics)

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
        val offsetRange = offsetRanges(TaskContext.getPartitionId())

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
            val connection = HBaseUtils.getConnection
            val table = connection.getTable(TableName.valueOf("doit.spark_kafka_order"))
            val puts = new java.util.ArrayList[Put](1000)

            iter.foreach(orderBean => {
              //设置数据，包括rk
              val put = new Put(Bytes.toBytes(orderBean.oid))
              //设置列族的数据
              put.addColumn(Bytes.toBytes("orders"), Bytes.toBytes("cid"), Bytes.toBytes(orderBean.cid))
              put.addColumn(Bytes.toBytes("orders"), Bytes.toBytes("money"), Bytes.toBytes(orderBean.money))
              put.addColumn(Bytes.toBytes("orders"), Bytes.toBytes("longitude"), Bytes.toBytes(orderBean.longitude))
              put.addColumn(Bytes.toBytes("orders"), Bytes.toBytes("latitude"), Bytes.toBytes(orderBean.latitude))

              if (!iter.hasNext) {
                put.addColumn(Bytes.toBytes("offsets"), Bytes.toBytes("groupid"), Bytes.toBytes(groupid))
                put.addColumn(Bytes.toBytes("offsets"), Bytes.toBytes("topic"), Bytes.toBytes(offsetRange.topic))
                put.addColumn(Bytes.toBytes("offsets"), Bytes.toBytes("partition"), Bytes.toBytes(offsetRange.partition))
                put.addColumn(Bytes.toBytes("offsets"), Bytes.toBytes("offset"), Bytes.toBytes(offsetRange.untilOffset))
              }

              //将put放入到puts这个list中
              puts.add(put)
              if (puts.size() == 1000) {
                //将数据写入到Hbase中
                table.put(puts)
                //清空puts集合中的数据
                puts.clear()
              }
            })

            //将没有达到100的数据也写入到Hbase中
            if (puts.size() > 0) {
              table.put(puts)
            }
            //关闭Hbase连接
            connection.close()
          })
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
