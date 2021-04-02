package bigdata.hermesfuxi.flink.windows.triggers;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.util.Collector;

/**
 * 创建一个Trigger，它在20秒内第一次触发并且在此后每隔5秒触发一次
 */
public class PurgingTriggerDemo {
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

        // 每个key分组都会两个、两个的触发
        keyedStream.window(GlobalWindows.create()).trigger(CountTrigger.of(2)).sum(1).print("countWithoutPurging");

        // PurgingTrigger: 当窗口触发时，会清除以前的状态，不会累计
        keyedStream.window(GlobalWindows.create()).trigger(PurgingTrigger.of(CountTrigger.of(2))).sum(1).print("countWithPurging");
        // PurgingTrigger是一个trigger的包装类。具体作用为：如果被包装的trigger触发返回FIRE，则PurgingTrigger将返回修改为FIRE_AND_PURGE，其他的返回值不做处理。

        /* 结果：
countWithoutPurging:8> (c,16)
countWithoutPurging:9> (d,30)
countWithPurging:8> (c,2)
countWithPurging:9> (d,2)
         */

        env.execute();
    }


}