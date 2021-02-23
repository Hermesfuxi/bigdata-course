package bigdata.hermesfuxi.spark.example.teacher

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object TeacherTopNDemo03 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("D:\\WorkSpaces\\IdeaProjects\\scala-course\\scala-spark\\src\\main\\resources\\topn\\teacher.log")
    // 数据处理聚合
    // http://bigdata.51doit.cn/laoduan -> bigdata
    val rdd2: RDD[((String, String), Int)] = rdd1.map(item => {
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

    val rdd3 = rdd2.groupBy(_._1._1)

    val result = rdd3.mapValues(values => {
      val tree = new mutable.TreeSet[((String, String), Int)]()(Ordering[Int].on[((String, String), Int)](-_._2))
      for (value <- values) {
        tree.add(value)
        if (tree.size > 3) {
          tree.remove(tree.last)
        }
      }
      tree
    })
    println(result.collect().toBuffer)

    sc.stop()
  }
}
