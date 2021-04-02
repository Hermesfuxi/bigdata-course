package bigdata.hermesfuxi.flink.windows.triigers

import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object DeltaTriggerDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //创建DeltaTrigger
    val deltaTrigger = DeltaTrigger.of[(String, Double), GlobalWindow](2.0, new DeltaFunction[(String, Double)] {
      override def getDelta(oldDataPoint: (String, Double), newDataPoint: (String, Double)): Double = {
        newDataPoint._2 - oldDataPoint._2
      }
    }, createTypeInformation[(String, Double)].createSerializer(env.getConfig))


    val lines = env.socketTextStream("hadoop-slave",9999)

    lines.map(_.split(","))
      .map(ts=>(ts(0),ts(1).toDouble))
      .keyBy(0)
      .window(GlobalWindows.create())
      .trigger(deltaTrigger)
      .reduce((v1:(String,Double),v2:(String,Double))=>(v1._1,v1._2+v2._2))
      .print()

    env.execute("Global Window Stream WordCount")
  }
}
