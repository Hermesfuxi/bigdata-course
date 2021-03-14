package bigdata.hermesfuxi.spark.utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.kafka.common.TopicPartition
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
 * 关于spark 整合 kafka 的工具类
 */
object KafkaSparkUtils {
  val kafkaBrokerList = "hadoop-master:9092,hadoop-master2:9092,hadoop-slave1:9092,hadoop-slave2:9092,hadoop-slave3:9092"


  /**
   * 从 Mysql 读取偏移量
   */
  def getOffsetMapFromMysql(group_id: String, topics: String) = {
    var connection: Connection = null
    var ps: PreparedStatement = null
    var resultSet: ResultSet = null
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    try {
      connection = MysqlUtils.getConnection
      ps = connection.prepareStatement("select * from kafka_offset where group_id=? and topic in (?)")
      ps.setString(1, group_id)
      ps.setString(2, topics)
      resultSet = ps.executeQuery()
      while (resultSet.next()) {
        val topicPartition = new TopicPartition(resultSet.getString("topic"), resultSet.getInt("partition"))
        val offset = resultSet.getLong("offset")
        offsetMap(topicPartition) = offset
      }
    } catch {
      case exception: Exception => exception.printStackTrace()
    } finally {
      if (resultSet != null) {
        resultSet.close()
      }
      if (ps != null) {
        ps.close()
      }
      if (connection != null) {
        connection.close()
      }
    }
    offsetMap.toMap
  }

  /**
   * 从 HBase 读取偏移量
   */
  def getOffsetMapFromHBase(groupid: String, topics: String) = {
    var connection: Connection = null
    var ps: PreparedStatement = null
    var resultSet: ResultSet = null
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    try {
      connection = DriverManager.getConnection("jdbc:phoenix:hadoop-master,hadoop-master2,hadoop-slave1,hadoop-slave2,hadoop-slave3:2181")
      // 分组求max，就是求没有分分区最（大）新的偏移量
      ps = connection.prepareStatement("select \"topic\", \"partition\", max(\"offset\") as \"offset\" from \"doit.spark_kafka_order\" where \"groupid\" = ?  and \"topic\" in (?) group by \"topic\", \"partition\"")

      ps.setString(1, groupid)
      ps.setString(2, topics)
      resultSet = ps.executeQuery()
      while (resultSet.next()) {
        val topic: String = resultSet.getString("topic")
        val partition: Int = resultSet.getInt("partition")
        val offset: Long = resultSet.getLong("offset")
        offsetMap(new TopicPartition(topic, partition)) = offset
      }
    } catch {
      case exception: Exception => exception.printStackTrace()
    } finally {
      if (resultSet != null) {
        resultSet.close()
      }
      if (ps != null) {
        ps.close()
      }
      if (connection != null) {
        connection.close()
      }
    }
    offsetMap.toMap
  }

  def getOffsetMapFromRedis(groupId: String, topics: String) = {
    var jedis: Jedis = null
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    jedis = RedisUtils.getRedisClient
    val map: java.util.Map[String, String] = jedis.hgetAll("kafka_offset" + "_" + groupId)
    // 导入隐式转换
    import scala.collection.JavaConverters._

    for (tp <- map.asScala) {
      val topic_partition = tp._1
      val offset = tp._2.toLong

      val fields = topic_partition.split("_")
      val topic = fields(0)
      val partition = fields(1).toInt
      if(topics.contains(topic)){
        val topicPartition = new TopicPartition(topic, partition)
        offsetMap(topicPartition) = offset
      }
    }
    offsetMap.toMap
  }
}
