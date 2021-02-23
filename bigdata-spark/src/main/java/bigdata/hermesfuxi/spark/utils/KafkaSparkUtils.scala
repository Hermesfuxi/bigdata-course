package bigdata.hermesfuxi.spark.utils

import java.sql.{Connection, PreparedStatement, ResultSet}

import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

/**
 * 关于spark 整合 kafka 的工具类
 */
object KafkaSparkUtils {
  val kafkaBrokerList = "hadoop-master:9092,hadoop-master2:9092,hadoop-slave1:9092,hadoop-slave2:9092,hadoop-slave3:9092"


  /**
   * 从数据库读取偏移量
   */
  def getOffsetMap(group_id: String, topics: Array[String]) = {
    var connection: Connection = null
    var ps: PreparedStatement = null
    var resultSet: ResultSet = null
    val offsetMap: mutable.Map[TopicPartition, Long] = null
    try {
      connection = MysqlUtils.getConnection
      ps = connection.prepareStatement("select * from kafka_offset where group_id=? and topic in (?)")
      ps.setString(1, group_id)
      ps.setString(2, topics.mkString(","))
      resultSet = ps.executeQuery()
      while (resultSet.next()) {
        offsetMap += new TopicPartition(resultSet.getString("topic"), resultSet.getInt("partition")) -> resultSet.getLong("offset")
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
    offsetMap
  }
}
