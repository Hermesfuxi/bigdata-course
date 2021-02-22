package bigdata.hermesfuxi.spark.example.login

import bigdata.hermesfuxi.spark.utils.{DateUtils, SparkUtils}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object LoginDemo01 {
  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getContext(true)
    val rdd1: RDD[String] = sparkContext.textFile("data/data1.txt")
    val rdd2: RDD[(String, String)] = rdd1.distinct().map(item=>{
      val strArr = item.split(",")
      if(strArr.length < 2){
        ("null", "null")
      }else {
        (strArr(0), strArr(1))
      }
    }).filter(_._1 != "null")
    val rdd3 = rdd2.groupByKey().mapValues(valueList =>{
      val sortedlist = valueList.toList.sorted
      var count = 0
      val newList = ListBuffer[(String, String)]()
      for(str <- sortedlist){
        val baseDataStr = DateUtils.getPlusFormatDate(count, str, "yyyy-MM-dd")
        count -= 1
        newList += new Tuple2[String, String](str, baseDataStr)
      }
      newList.groupBy(_._2).mapValues(listBuffer => {
        val listTemp = listBuffer.map(_._1)
        (listTemp.length, listTemp.min, listTemp.max)
      }).values.filter(_._1 >= 3)
    })
    println(rdd3.collect().toBuffer)
    sparkContext.stop()
  }
}
