package bigdata.hermesfuxi.spark.advance.accumlator

import bigdata.hermesfuxi.spark.utils.SparkUtils

// 全局变量-闭包问题 引出 累加器
//累加器本质就是在Driver端初始化的一个类的实例,并且在函数内部使用，存在闭包现象，每个task都有自由的一个计数器引用
object AccumulatorDemo01 {
  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getContext(true)
    val arr = Array(1,2,3,4,5, 6,7,8,9,10)
    val arrRdd = sparkContext.parallelize(arr, 3)
    val countAccumulator = sparkContext.longAccumulator(this.getClass.getName)
//    val countAccumulator = new LongAccumulator
//    sparkContext.register(countAccumulator, this.getClass.getName)

    val resultRdd = arrRdd.map(item => {
      if (item % 2 == 0) {
        countAccumulator.add(1)
      }
      item * 10
    })
    println("触发Action之前：" + countAccumulator.count)
    println(resultRdd.collect().toBuffer)
    println("触发Action之后：" + countAccumulator.count)
    sparkContext.stop()
  }
}
