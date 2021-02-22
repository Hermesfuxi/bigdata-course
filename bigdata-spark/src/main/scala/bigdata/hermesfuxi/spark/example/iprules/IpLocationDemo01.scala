package bigdata.hermesfuxi.spark.example.iprules

import bigdata.hermesfuxi.spark.utils.SparkUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object IpLocationDemo01 {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = SparkUtils.getContext(true)

    //使用spark读取IP规则数据
    val ipLines: RDD[String] = sc.textFile("data/ip.txt")
    val ipRulesRDD: RDD[(Long, Long, String, String)] = ipLines.map(e => {
      val fields = e.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      val city = fields(7)
      (startNum, endNum, province, city)
    })
    //将整理后的IP规则收集到Driver端

    //将Driver的IP规则数据广播到Executor中，该方法是一个阻塞的方法

    //读取访问日志数据


    sc.stop()
  }
}
