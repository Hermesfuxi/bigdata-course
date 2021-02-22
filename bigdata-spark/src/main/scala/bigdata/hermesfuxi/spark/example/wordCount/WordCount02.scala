package bigdata.hermesfuxi.spark.example.wordCount

import java.sql.{Connection, Date, DriverManager, PreparedStatement, SQLException}

import bigdata.hermesfuxi.spark.example.wordCount.dao.OrderData
import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.{ClassTag, classTag}

object WordCount02 {
  def validJson[T: ClassTag](jsonStr: String): Boolean = {
    val clazz = classTag[T].runtimeClass // 正确的获取T的泛型方式
    try {
      JSON.parseObject(jsonStr, clazz)
      true
    } catch {
      case ex: Exception => println(s"JSON解析错误，jsonStr：${jsonStr}"); false
    }
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    // 数据为 JSON 格式，且存在不规范数据
    // "oid":"o124", "cid": 2, "money": 200.0, "longitude":117.397128,"latitude":38.916527
    val lines: RDD[String] = sc.textFile("D:\\WorkSpaces\\IdeaProjects\\scala-course\\scala-spark\\src\\main\\resources\\order.log")
    // 在给定的订单数据，按照订单分类CID，聚合统计出某一天商品各个分类的成交金额，并联合分类名称，计算结果保存MySQL

    // 1、数据清洗
    // 2、对订单分类：以OID进行聚合
    val result1: RDD[(String, Double)] = lines.filter(validJson[OrderData]).map(line => {
      val data = JSON.parseObject(line, classOf[OrderData])
      (data.cid, data.money)
    }).reduceByKey(_ + _).sortBy(_._2, ascending = false)

    println(result1.collect().toBuffer)

    val lines2: RDD[String] = sc.textFile("D:\\WorkSpaces\\IdeaProjects\\scala-course\\scala-spark\\src\\main\\resources\\cid.log")
    val result2: RDD[(String, String)] = lines2.filter(_.split(",").length > 1).map(line => {
      val strs = line.split(",")
      println(strs.toBuffer, "=====================")
      (strs(0), strs(1))
    })
    println(result2.collect().toBuffer)

    val result: RDD[(String, Double)] = result1.join(result2).map(tuple => (tuple._2._2, tuple._2._1))
    println(result.collect().toBuffer)
//    result.foreachPartition(dataToMySQL)
    sc.stop();
  }

  val dataToMySQL: Iterator[(String, Double)] => Unit = (it: Iterator[(String, Double)]) => {
    //创建MySQL连接
    var conn: Connection = null
    var statement: PreparedStatement = null
    try {
      conn = DriverManager.getConnection(
        "jdbc:mysql://localhost:3306/spark?characterEncoding=UTF-8&serverTimezone=GMT%2B8&useSSL=false",
        "root",
        "123456")
      statement = conn.prepareStatement("INSERT INTO t_result values (null, ?, ?, ?)")
      //将迭代器中的数据写入到MySQL
      it.foreach(t => {
        statement.setString(1, t._1)
        statement.setDouble(2, t._2)
        statement.setDate(3, new Date(System.currentTimeMillis()))
        //执行
        //statement.executeUpdate()
        //批量写入
        statement.addBatch()
      })
      statement.executeBatch()
      ()
    } catch {
      case e: SQLException => {
        //单独处理有问题的数据
        e.printStackTrace()
        ()
      }
    } finally {
      //释放MySQL的资源
      if (statement != null) {
        statement.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

}
