package bigdata.hermesfuxi.spark.example.wordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCount")
    val sc = new SparkContext(conf)

    // start、创建RDD：指定输入
    val lines: RDD[String] = sc.textFile("E:\\Documents\\百科全书\\计算机\\大数据\\多易教育\\mrdata\\wordcount\\input\\words.txt")
    val words: RDD[String] = lines.flatMap(_.split("\\s+"))
    val wordValues: RDD[(String, Int)] = words.map((_, 1))
    val values: RDD[(String, Int)] = wordValues.reduceByKey(_ + _)
    val sortedValues: RDD[(String, Int)] = values.sortBy(_._2, ascending = false)
    sortedValues.saveAsTextFile("E:\\Documents\\百科全书\\计算机\\大数据\\多易教育\\mrdata\\wordcount\\output")
    // end: 指定输出
    sc.stop()
  }

}
