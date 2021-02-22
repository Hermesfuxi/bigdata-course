package bigdata.hermesfuxi.spark.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 过程大概这样：
 * 首先，初始值是(0,0)，这个值在后面2步会用到。
 * 然后，(acc,number) => (acc._1 + number, acc._2 + 1)，number即是函数定义中的T，这里即是List中的元素。
 * 所以acc._1 + number,acc._2 + 1的过程如下:
 * 1.  0+1,  0+1
 * 2.  1+2,  1+1
 * 3.  3+3,  2+1
 * 4.  6+4,  3+1
 * 5.  10+5,  4+1
 * 6.  15+6,  5+1
 * 7.  21+7,  6+1
 * 8.  28+8,  7+1
 * 9.  36+9,  8+1
 * 实际Spark执行中是分布式计算，可能会把List分成多个分区，假如3个，p1(1,2,3,4)，p2(5,6,7,8)，p3(9)，
 * 经过计算各分区的的结果（10,4）,（26,4）,（9,1），这样，执行(par1,par2) =>(par1._1 + par2._1, par1._2 + par2._2)
 * 就是（10+26+9,4+4+1）即(45,9)
 */
object AggregateDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd5 = List(1, 2, 3, 4, 5, 6, 7, 8, 9)

    val tuple = sc.parallelize(rdd5).aggregate((0, 0))(
      (acc, number) => {
        println(acc._1)
        (acc._1 + number, acc._2 + 1)
      },
      (par1, par2) => (par1._1 + par2._1, par1._2 + par2._2)
    )
    println(tuple)

    sc.stop()
  }
}
