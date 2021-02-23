package bigdata.hermesfuxi.spark.operator.transformation

import org.apache.spark.{SparkConf, SparkContext}

object SubtracDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(5,6,4,7))
    val rdd2 = sc.parallelize(List(1,2,3,4))
    val value = rdd1.subtract(rdd2)
    sc.stop()
  }
}
