package bigdata.hermesfuxi.spark.operator.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object JoinDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(("tom", 1), ("jerry", 2), ("kitty", 3)))
    val rdd2 = sc.parallelize(List(("jerry", 9), ("tom", 8), ("shuke", 7), ("tom", 2)))

    val rdd3 = rdd1.join(rdd2)
    // ArrayBuffer((tom,(1,8)), (tom,(1,2)), (jerry,(2,9)))
    println(rdd3.collect().toBuffer)

    val cgResult: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
    // ArrayBuffer(
    // (tom,(CompactBuffer(1),CompactBuffer(8, 2))),
    // (shuke,(CompactBuffer(),CompactBuffer(7))),
    // (kitty,(CompactBuffer(3),CompactBuffer())),
    // (jerry,(CompactBuffer(2),CompactBuffer(9)))
    // )
    println(cgResult.collect().toBuffer)

    cgResult.flatMap(item => {
      for (a <- item._2._1; b <- item._2._1) yield (item._1, (a, b))
    }).collect().foreach(println)

    val list = List(("hadoop", 3), ("hadoop", 2), ("spark", 4), ("spark", 3), ("storm", 6), ("storm", 8))
    var result = sc.parallelize(list,numSlices = 2).aggregateByKey(zeroValue = 0,numPartitions = 1)(
      seqOp = math.max(_, _),
      combOp = _ + _
    ).collect().foreach(println)




    sc.stop()

  }

}
