package bigdata.hermesfuxi.flink.windows;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author Hermesfuxi
 *  不分组，按照ProcessingTime划分滚动窗口
 *  Non-keyed Window，window和window function所在的DataStream并行度为1
 *  windowAll(Window Assinger) 并行度为 1
 */
public class ProcessingTimeWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> source = environment.socketTextStream("hadoop-slave3", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] strings = value.split("\\s+");
                return Tuple2.of(strings[0], Integer.valueOf(strings[1]));
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = operator.keyBy(t -> t.f0);
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
//        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)));
//        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = window.sum(1);
        result.print();
        environment.execute();
    }
}
