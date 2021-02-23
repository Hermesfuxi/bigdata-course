package bigdata.hermesfuxi.spark.operator.special.cache

import bigdata.hermesfuxi.spark.utils.SparkUtils
import org.apache.spark.rdd.RDD

object CacheDemo01 {
  def main(args: Array[String]): Unit = {
    val context = SparkUtils.getContext(true)

    val rdd1: RDD[Int] = context.makeRDD(List(1, 2, 3, 4))
    println(s"rdd1: ${rdd1}")
    val rdd1Collect = rdd1.collect()
    println(rdd1Collect.toBuffer)

    rdd1.cache()

    val rdd2 = rdd1.map(_ * 10)
    println(s"rdd2: ${rdd2}")
    val rdd2Collect = rdd2.collect()
    println(rdd2Collect.toBuffer)

    context.stop()
  }
}
