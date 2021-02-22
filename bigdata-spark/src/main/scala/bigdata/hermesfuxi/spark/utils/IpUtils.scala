package bigdata.hermesfuxi.spark.utils

import scala.collection.mutable.ArrayBuffer

object IpUtils {

  /**
    * 将IP地址转成十进制
    *
    * @param ip
    * @return
    */
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  /**
    * 二分法查找
    *
    * @param lines
    * @param ip
    * @return
    */
  def binarySearch(lines: ArrayBuffer[(Long, Long, String, String)], ip: Long): Int = {
    var low = 0 //起始
    var high = lines.length - 1 //结束
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1 //没有找到
  }

  def binarySearch(lines: Array[(Long, Long, String, String)], ip: Long): Int = {
    var low = 0 //起始
    var high = lines.length - 1 //结束
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1 //没有找到
  }
}
