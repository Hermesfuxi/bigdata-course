package bigdata.hermesfuxi.spark.example.teacher

import java.io.{File, PrintWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

//自定义分区去，保证一个分区内有且仅有一个学科的数据
//在调用reduceByKey时就使用自定义的分区器
object TeacherTopNDemo05 {
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
    }).filter(_._1 != null)
    rdd2.cache()
    val strArr: Array[String] = rdd2.map(_._1._1).distinct().collect()
    val rdd3: RDD[((String, String), Int)] = rdd2.reduceByKey(new InitPartitioner02(strArr), _ + _)

    rdd3.foreachPartition(iter=>{
      val treeSet = new mutable.TreeSet[((String, String), Int)]()(Ordering[Int].on[((String, String), Int)](-_._2))
      while (iter.hasNext) {
        val tuple = iter.next()
        treeSet.add(tuple)
        if (treeSet.size > 2) {
          treeSet.remove(treeSet.last)
        }
      }
      println(treeSet.toBuffer)
//      val file = new File("scala-spark/src/main/resources/test.txt")
//      if(!file.exists()){
//        file.createNewFile()
//      }
//      val writer = new PrintWriter(file)
//      writer.write(treeSet.toBuffer.toString())
//      writer.close()
    })
    sc.stop()
  }
}

class InitPartitioner02(val initArray: Array[String]) extends Partitioner{
  require(initArray != null && initArray.length >= 0, s"Number of partitions (${initArray.length}) cannot be negative.")
  //定义一个分区的规则
  var index = 0
  var name2Index = new mutable.HashMap[String, Int]()
  for (name <- initArray) {
    name2Index(name) = index
    index += 1
  }

  def getPartition(key: Any): Int = {
    val name = key.asInstanceOf[(String, String)]
    name2Index(name._1)
  }

  override def numPartitions: Int = initArray.length

}
