package bigdata.hermesfuxi.spark.operator.transformation

import org.apache.spark.{SparkConf, SparkContext}

object ReduceByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val data = List((1, 3), (1, 2), (1, 4), (2, 3))

    val rdd = sc.parallelize(data, 2)

    val result = rdd.reduceByKey(_+_)
    println(result.collect().toBuffer)
    sc.stop()
  }
}
