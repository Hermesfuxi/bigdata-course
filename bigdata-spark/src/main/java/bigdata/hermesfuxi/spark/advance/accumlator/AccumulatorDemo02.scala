package bigdata.hermesfuxi.spark.advance.accumlator

import bigdata.hermesfuxi.spark.utils.SparkUtils

object AccumulatorDemo02 {
  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getContext(true)
    val arr = Array(1,2,3,4,5, 6,7,8,9,10)
    val arrRdd = sparkContext.parallelize(arr, 3)
    val countAccumulator = sparkContext.longAccumulator(this.getClass.getName)
    val resultRdd = arrRdd.map(item => {
      if (item % 2 == 0) {
        countAccumulator.add(1)
      }
      item * 10
    })
    resultRdd.saveAsTextFile("data/acc2")
    println("第一次触发Action：" + countAccumulator.count)
    println(resultRdd.collect().toBuffer)

    // 再未加缓存前，结果并不一致
    println("第二次Action之后：" + countAccumulator.count)
    sparkContext.stop()
  }
}
