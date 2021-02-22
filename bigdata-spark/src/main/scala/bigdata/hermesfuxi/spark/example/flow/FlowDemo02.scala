package bigdata.hermesfuxi.spark.example.flow

import bigdata.hermesfuxi.spark.utils.{DateUtils, SparkUtils}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object FlowDemo02 {
  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getContext(true)
    val rdd1: RDD[String] = sparkContext.textFile("data/flow.txt")
    val rdd2 = rdd1.distinct().map(item=>{
      val strArr = item.split(",")
      if(strArr.length < 2){
        ("null", "null", "null", 0)
      }else {
        (strArr(0), strArr(1), strArr(2), strArr(3).toInt)
      }
    }).filter(_._1 != "null")
    val rdd3 = rdd2.groupBy(_._1).sortBy(_._1).mapValues(valueList =>{
      val sortedlist = valueList.toList.sortBy(_._2)
      // 设定中间值
      var oldStartTime = ""
      var oldEndTime = ""
      var flowCount = 0
      var mapCount = 0

      val listBuffer = ListBuffer[(String, String, Int)]()

      sortedlist.foreach(item=>{
        if(mapCount == 0){
          oldStartTime = item._2
          oldEndTime = item._3
          flowCount += item._4
        }else{
          val newStartTimeLong = DateUtils.getLongTime(item._2, "yyyy-MM-dd HH:mm:ss")
          val oldEndTimeLong = DateUtils.getLongTime(oldEndTime, "yyyy-MM-dd HH:mm:ss")
          if(newStartTimeLong - oldEndTimeLong >= 10 * 60){
            // 大于等于10min，再分一组，将前一组纪录下来，并清空中间值
            listBuffer += Tuple3(oldStartTime, oldEndTime, flowCount)
            oldStartTime = item._2
            oldEndTime = item._3
            flowCount += item._4
          }else{
            // 小于10min，更新中间值
            oldEndTime = item._3
            flowCount += item._4
          }
        }

        if(mapCount == sortedlist.length - 1){
          // 最后一次遍历，都会存在已有分组，直接清出
          listBuffer += Tuple3(oldStartTime, oldEndTime, flowCount)
          oldStartTime = ""
          oldEndTime = ""
        }
        mapCount += 1
      })
      listBuffer
    })
    println(rdd3.collect().toBuffer)
    sparkContext.stop()
  }
}
