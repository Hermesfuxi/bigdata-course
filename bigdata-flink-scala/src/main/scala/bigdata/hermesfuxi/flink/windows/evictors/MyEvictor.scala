package bigdata.hermesfuxi.flink.windows.evictors

import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue

class MyEvictor() extends Evictor[Long, TimeWindow] {
  override def evictBefore(iterable: java.lang.Iterable[TimestampedValue[Long]], i: Int, w: TimeWindow, evictorContext: Evictor.EvictorContext): Unit = {
    val ite: java.util.Iterator[TimestampedValue[Long]] = iterable.iterator()
    while (ite.hasNext) {
      val element: TimestampedValue[Long] = ite.next()
      //指定事件事件获取到的就是事件时间
      println("驱逐器获取到的时间：" + element.getTimestamp)
      //模拟去掉非法参数数据
      if (element.getValue <= 0) {
        ite.remove()
      }
    }
  }

  override def evictAfter(iterable: java.lang.Iterable[TimestampedValue[Long]], i: Int, w: TimeWindow, evictorContext: Evictor.EvictorContext): Unit = {

  }
}