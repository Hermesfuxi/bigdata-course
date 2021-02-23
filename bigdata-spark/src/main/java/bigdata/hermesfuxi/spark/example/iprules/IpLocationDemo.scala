package bigdata.hermesfuxi.spark.example.iprules

import bigdata.hermesfuxi.spark.utils.{IpUtils, SparkUtils}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object IpLocationDemo {

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
    var ipRulesInDriver: Array[(Long, Long, String, String)] = ipRulesRDD.collect()

    //将Driver的IP规则数据广播到Executor中，该方法是一个阻塞的方法
    val broadcastRef: Broadcast[Array[(Long, Long, String, String)]] = sc.broadcast(ipRulesInDriver)

    //读取访问日志数据
    val accessLog: RDD[String] = sc.textFile(args(2))

    val provinceAndOne: RDD[(String, Int)] = accessLog.map(e => {

      val fields = e.split("[|]")
      val ip = fields(1)
      val ipNum = IpUtils.ip2Long(ip)
      //通过Driver返回的广播变量引用，获取事先已经广播到当前Executor中的数据
      val ipRulesInExecutor: Array[(Long, Long, String, String)] = broadcastRef.value
      //进行二分法查找
      val index = IpUtils.binarySearch(ipRulesInExecutor, ipNum)
      var province = "未知"
      if(index >= 0) {
        province = ipRulesInExecutor(index)._3
      }
      (province, 1)
    })

    val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey(_ + _)

    val res = reduced.collect()

    println(res.toBuffer)

    broadcastRef.unpersist(true)

    //使用spark读取IP规则数据
    val ipLines2: RDD[String] = sc.textFile(args(1))
    val ipRulesRDD2: RDD[(Long, Long, String, String)] = ipLines.map(e => {
      val fields = e.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      val city = fields(7)
      (startNum, endNum, city, province)
    })

    ipRulesInDriver = ipRulesRDD2.collect()

    val broadCastRef: Broadcast[Array[(Long, Long, String, String)]] = sc.broadcast(ipRulesInDriver)

    val provinceAndOne2: RDD[(String, Int)] = accessLog.map(e => {

      val fields = e.split("[|]")
      val ip = fields(1)
      val ipNum = IpUtils.ip2Long(ip)
      //通过Driver返回的广播变量引用，获取事先已经广播到当前Executor中的数据
      val ipRulesInExecutor: Array[(Long, Long, String, String)] = broadCastRef.value
      //进行二分法查找
      val index = IpUtils.binarySearch(ipRulesInExecutor, ipNum)
      var province = "未知"
      if(index >= 0) {
        province = ipRulesInExecutor(index)._3
      }
      (province, 1)
    })

    val reduced2: RDD[(String, Int)] = provinceAndOne2.reduceByKey(_ + _)

    val res2 = reduced2.collect()

    println(res2.toBuffer)

    sc.stop()
  }
}
