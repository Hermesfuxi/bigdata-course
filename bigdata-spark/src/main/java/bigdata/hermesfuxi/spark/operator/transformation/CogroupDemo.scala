package bigdata.hermesfuxi.spark.operator.transformation

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object CogroupDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(("tom", 1), ("jerry", 2), ("kitty", 3)))
    val rdd2 = sc.parallelize(List(("jerry", 9),("shuke", 7), ("tom", 8),  ("tom", 2)))

    val tuples = rdd1.top(2)
    println(tuples.toBuffer)

    val tuples1 = rdd2.take(2)
    println(tuples1.toBuffer)

//    val result = rdd1.cogroup(rdd1, new HashPartitioner(3))

//    val result: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
//    println(result.collect.toBuffer)
    sc.stop()
  }
}
