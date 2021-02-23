package bigdata.hermesfuxi.spark.example.wordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @auther : hermesfuxi
 * @date : 2020/12/21 0021
 * @description :
 */
object WordCount01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCount01")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(args(0))
    val values: RDD[(String, Int)] = lines.flatMap(_.split("\\s+")).map((_, 1))
    val result = values.reduceByKey(_ + _).sortBy(_._2, ascending = false)
    result.saveAsTextFile(args(1))
    sc.stop();
  }
}
