package bigdata.hermesfuxi.spark.example.sale

import bigdata.hermesfuxi.spark.utils.{DateUtils, SparkUtils}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object SaleDemo01 {
  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getContext(true)
    val rdd1: RDD[String] = sparkContext.textFile("data/shop.csv")
    val rdd2 = rdd1.distinct().map(item=>{
      val strArr = item.split(",")
      if(item == "sid,dt,money" || strArr.length < 3){
        ("null", "null", "null")
      }else {
        (strArr(0), strArr(1), strArr(2))
      }
    }).filter(_._1 != "null")

    val rdd3 = rdd2.groupBy(_._1).mapValues(values=>{
      val shopMapValues: Map[Int, Int] = values.toList.sortBy(_._2).map(item => {
        (DateUtils.getDateMonth(item._2, "yyyy-MM-dd"), item._3.toInt)
      }).groupBy(_._1).mapValues(_.map(_._2).sum).map(x => x)
//      shopMapValues
//      ArrayBuffer((shop2,Map(2 -> 300, 4 -> 100, 3 -> 100)), (shop1,Map(2 -> 2000, 4 -> 500, 1 -> 500, 3 -> 180)))

      var count = 0
      shopMapValues.map(item=>{
        count += item._2
        (item._1, item._2, count)
      })
      // ArrayBuffer((shop2,List((2,300,300), (4,100,400), (3,100,500))), (shop1,List((2,2000,2000), (4,500,2500), (1,500,3000), (3,180,3180))))
    })
    println(rdd3.collect().toBuffer)
    sparkContext.stop()
  }
}
