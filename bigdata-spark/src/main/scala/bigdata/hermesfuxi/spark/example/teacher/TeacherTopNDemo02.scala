package bigdata.hermesfuxi.spark.example.teacher

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TeacherTopNDemo02 {
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
    val keys = rdd2.map(_._1._1).distinct().collect()
    for(key <- keys){
      val tuples = rdd2.filter(_._1._1 == key).top(2)(Ordering[Int].on[((String, String), Int)](-_._2))
      println(tuples.toBuffer)
    }
    sc.stop()
  }
}
