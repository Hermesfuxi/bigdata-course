package bigdata.hermesfuxi.spark.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object ReduceDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val data = List((1, 3), (1, 2), (1, 4), (2, 3))

    val rdd = sc.parallelize(data, 2)

//    val result = rdd.reduce((a,b) => )
//    println(result)
    sc.stop()
  }
}
