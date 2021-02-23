package bigdata.hermesfuxi.spark.advance.broadcast

import bigdata.hermesfuxi.spark.utils.SparkUtils

object BroadCastDemo01 {
  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getContext(true)
    val map1 = Map("name" -> "hermesfuxi")
    val map2 = Map("name" -> "tangguoxin")
    val broadcastMap1 = sparkContext.broadcast(map1)
    val broadcastMap2 = sparkContext.broadcast(map2)

    val ipLines = 1 to 10

    val result = sparkContext.makeRDD(ipLines).map(item => {
      println(broadcastMap1.id)
      println(broadcastMap2.id)
      println(broadcastMap1.value.toBuffer)
      println(broadcastMap2.value.toBuffer)
      item
    })
    result.collect()
    sparkContext.stop()
  }
}
