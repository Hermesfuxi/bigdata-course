package bigdata.hermesfuxi.spark.advance.accumlator

import bigdata.hermesfuxi.spark.utils.SparkUtils
import com.alibaba.fastjson.JSON

object CollectionAccumulatorDemo01 {
  def main(args: Array[String]): Unit = {
    val context = SparkUtils.getContext(true)
    val list = List("\"name\" : \"laoduan\", \"age\" : 35, \"fv\": 99.99}", "{\"name\" : \"nianhang\", \"age\" : 30, \"fv\": 99.99}", "{\"name\" : \"laozhao\", \"age\" : 34, \"fv\": 9999.99}", "{\"name\" : \"laoduan\", \"age\" : 35, \"fv\": 99.99}", "{\"name\" : \"nianhang\", \"age\" : 30, \"fv\": 99.99}", "{\"name\" : \"laozhao\", \"age\" : 34 \"fv\" 9999.99}")
    val lines = context.parallelize(list)
    //使用累加器
    val collectAccumulate = context.collectionAccumulator[String](this.getClass.getName)
    val result = lines.map(item => {
      try {
        JSON.parse(item)
      } catch {
        case e: Exception => {
          collectAccumulate.add(item)
        }
      }
    })
    println(result.collect().toBuffer)
    println(collectAccumulate.value)
    context.stop()
  }
}
