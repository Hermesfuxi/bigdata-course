package bigdata.hermesfuxi.spark.example.teacher

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object TeacherTopNDemo01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("D:\\WorkSpaces\\IdeaProjects\\scala-course\\scala-spark\\src\\main\\resources\\topn\\teacher.log")
    // 数据处理 http://bigdata.51doit.cn/laoduan -> bigdata
    val rdd2 = rdd1.map(item => {
      val strings = item.split("/")
      if (strings.length < 4) {
        (null, 1)
      } else {
        val clazz = strings(2).split("\\.")(0)
        val teacher = strings(3)
        ((clazz, teacher), 1)
      }
    }).filter(_._1 != null).reduceByKey(_+_)
    rdd2.cache()

    // 分组排序
    val rdd3: RDD[(String, Iterable[((String, String), Int)])] = rdd2.groupBy(_._1._1)
    val result = rdd3.mapValues(_.toList.sortBy(-_._2).take(1)).flatMap(_._2)

    println(result.collect().toBuffer)
    // 存储结果

    sc.stop()
  }
}
