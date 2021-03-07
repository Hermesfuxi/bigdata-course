package bigdata.hermesfuxi.flink.example.wordcount

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}

object StreamWordCountDemo1 {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val lines: DataStream[String] = environment.socketTextStream(args(0), args(1).toInt)

    val linesSplitWord = lines.flatMap(_.split("\\s+"))

    val wordAndOne = linesSplitWord.map((_, 1))

    val keyed = wordAndOne.keyBy(_._1)

    val result = keyed.sum(1)

    result.print()

    environment.execute(this.getClass.getSimpleName)
  }
}
