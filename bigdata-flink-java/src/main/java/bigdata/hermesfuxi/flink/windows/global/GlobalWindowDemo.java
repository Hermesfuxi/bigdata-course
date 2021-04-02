package bigdata.hermesfuxi.flink.windows.global;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeoutTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 创建一个Trigger，它在20秒内第一次触发并且在此后每隔5秒触发一次
 */
public class GlobalWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop-slave2", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split("\\s+");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = streamOperator.keyBy(t -> t.f0);

        keyedStream.window(GlobalWindows.create()).sum(1).print("未加Trigger");// 永远不会执行

        // 没有 Trigger， 不会触发，默认是 NeverTrigger
        keyedStream.window(GlobalWindows.create())
                // TODO 多个触发器调用会有用吗？
                .trigger(ProcessingTimeoutTrigger.of(CountTrigger.of(2), Duration.ofSeconds(10)))
                .sum(1).print("未加Trigger");

        env.execute();
    }


}