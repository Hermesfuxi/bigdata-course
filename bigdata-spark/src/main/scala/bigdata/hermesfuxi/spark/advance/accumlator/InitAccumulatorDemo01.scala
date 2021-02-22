package bigdata.hermesfuxi.spark.advance.accumlator

import java.util
import java.util.{ArrayList, Collections}

import bigdata.hermesfuxi.spark.utils.SparkUtils
import com.alibaba.fastjson.JSON
import org.apache.spark.util.AccumulatorV2

// 手写 CollectionAccumulator
object InitAccumulatorDemo01 {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getContext(true)
    val list = List("\"name\" : \"laoduan\", \"age\" : 35, \"fv\": 99.99}", "{\"name\" : \"nianhang\", \"age\" : 30, \"fv\": 99.99}", "{\"name\" : \"laozhao\", \"age\" : 34, \"fv\": 9999.99}", "{\"name\" : \"laoduan\", \"age\" : 35, \"fv\": 99.99}", "{\"name\" : \"nianhang\", \"age\" : 30, \"fv\": 99.99}", "{\"name\" : \"laozhao\", \"age\" : 34 \"fv\" 9999.99}")

    val accumulator = new InitAccumulator[String]
    sc.register(accumulator, this.getClass.getName)

    val rdd1 = sc.makeRDD(list)
    val rdd2 = rdd1.map(item => {
      try {
        JSON.parse(item)
      } catch {
        case e: Exception => {
          e.printStackTrace()
          accumulator.add(item)
          null
        }
      }
    }).filter(_ != null)
    println("第一次触发Action：" + accumulator.value)
    println(rdd2.collect().toBuffer)
    println("第二次触发Action：" + accumulator.value)
    sc.stop()
  }
}

class InitAccumulator[T] extends AccumulatorV2[T, java.util.List[T]] with Serializable {
  private val _list: java.util.List[T] = Collections.synchronizedList(new ArrayList[T]())

  /**
   * 判断，这个要和reset设定值一致
   */
  override def isZero: Boolean = {
    _list.size() == 0
  }

  // 深拷贝，用于多线程
  override def copy(): InitAccumulator[T] = {
    val newAccumulator = new InitAccumulator[T]
    _list.synchronized {
      newAccumulator._list.addAll(this._list)
    }
    newAccumulator
  }

  // 重置每个分区的数值
  override def reset(): Unit = {
    _list.clear()
  }

  // 每个分区累加自己的数值
  override def add(v: T): Unit = {
    _list.add(v)
  }

  // 合并分区值，求得总值
  override def merge(other: AccumulatorV2[T, java.util.List[T]]): Unit = other match {
    case o: InitAccumulator[T] => _list.addAll(o.value)
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: java.util.List[T] = _list.synchronized {
    // 返回不可修改的只读视图
    java.util.Collections.unmodifiableList(new util.ArrayList[T](_list))
  }

}
