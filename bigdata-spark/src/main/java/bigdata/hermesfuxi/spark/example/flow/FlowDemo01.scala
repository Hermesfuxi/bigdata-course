package bigdata.hermesfuxi.spark.example.flow

import bigdata.hermesfuxi.spark.utils.{DateUtils, SparkUtils}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object FlowDemo01 {
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
      println(sortedlist.toBuffer)
      // 设定中间值
      var oldStartTime = ""
      var oldEndTime = ""
      var flowCount = 0
      var groupCount = 0
      var mapCount = 0

      val listBuffer = ListBuffer[(String, String, Int, Int)]()

      sortedlist.foreach(item=>{
        // oldStartTime 的更新代表着新分组的开始
        if(mapCount == 0){
          oldStartTime = item._2
        }else{
          val newStartTimeLong = DateUtils.getLongTime(item._2, "yyyy-MM-dd HH:mm:ss")
          val oldEndTimeLong = DateUtils.getLongTime(oldEndTime, "yyyy-MM-dd HH:mm:ss")
          if(newStartTimeLong - oldEndTimeLong >= 10 * 60){
            // 大于等于10min，再分一组，将前一组纪录下来，并清空中间值
            // 小于10min，只更新中间值
            listBuffer += Tuple4(oldStartTime, oldEndTime, groupCount, flowCount)
            oldStartTime = item._2
            groupCount = 0
          }
        }
        // 必然更新终止时间
        oldEndTime = item._3
        flowCount += item._4
        groupCount += item._4

        if(mapCount == sortedlist.length - 1){
          // 最后一次遍历，都会存在已有分组，直接清出
          listBuffer += Tuple4(oldStartTime, oldEndTime, groupCount, flowCount)
        }
        mapCount += 1
      })
      listBuffer
    })
    println(rdd3.collect().toBuffer)
    sparkContext.stop()
  }
}
