package bigdata.hermesfuxi.spark.example.login

import bigdata.hermesfuxi.spark.utils.{DateUtils, SparkUtils}
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

// 使用自定义分区器
object LoginDemo03 {
  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getContext(true)
    val rdd1: RDD[String] = sparkContext.textFile("data/data1.txt")
    val rdd2 = rdd1.distinct().map(item=>{
      val strArr = item.split(",")
      if(strArr.length < 2){
        null
      }else {
        ((strArr(0), strArr(1)), null)
      }
    }).filter(_ != null)
    rdd2.cache()
    val rdd3 = rdd2.repartitionAndSortWithinPartitions(new MyHashPartitioner(3))
    val result = rdd3.mapPartitions(iter => {
      var count = 0
      iter.map(item => {
        val baseDataStr = DateUtils.getPlusFormatDate(count, item._1._2, "yyyy-MM-dd")
        count -= 1
        ((item._1._1, baseDataStr), (item._1._2, item._1._2, 1))
      })
    }).reduceByKey((a, b) => {
      (Ordering[String].min(a._1, b._1), Ordering[String].max(a._1, b._1), a._3 + b._3)
    }).map(item=> (item._1._1, item._2)).filter(_._2._3 >= 3)
    println(result.collect().toBuffer)
//    rdd3.saveAsTextFile("data/out1")
    sparkContext.stop()
  }
}

class MyHashPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    if(key == null){
      0
    }else{
      nonNegativeMod(key.asInstanceOf[(String, String)]._1.hashCode, partitions)
    }
  }

  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    val result = rawMod + (if (rawMod < 0) mod else 0)
    result
  }
}
