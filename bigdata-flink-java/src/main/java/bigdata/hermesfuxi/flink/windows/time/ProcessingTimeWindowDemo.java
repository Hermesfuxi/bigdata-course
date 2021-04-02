package bigdata.hermesfuxi.flink.windows.time;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
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
        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> operator = source.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(Long.parseLong(fields[0]), fields[1], Integer.valueOf(fields[2]));
            }
        });

        KeyedStream<Tuple3<Long, String, Integer>, String> keyedStream = operator.keyBy(t -> t.f1);
//        WindowedStream<Tuple3<Long, String, Integer>, String, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
//        WindowedStream<Tuple3<Long, String, Integer>, String, TimeWindow> window = keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)));

        // 会话窗口：会话窗口不重叠并且没有固定的开始和结束时间。当会话窗口在一段时间内没有接收到元素时，即当发生不活动的间隙时，会话窗口关闭
        //窗口触发的条件是Session Gap（会话间隙），Session Gap规定了不活跃数据的时间上限。会话窗口适用于非连续型数据处理或者周期性产生数据的场景
        // 通过withGap来指定不活跃数据的时间周期（本例10m）
//        WindowedStream<Tuple3<Long, String, Integer>, String, TimeWindow> window = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));

        //支持动态调整Session Gap
        // 由于Session Window本质上没有固定的起止时间，因此其底层计算逻辑和滑动、滚动窗口有一定的区别。
        // Session Windows会为每一个进入的数据都创建一个窗口，最后将距离Session Gap最近的窗口进行合并计算窗口结果。
        // 因此对于Session Windows来说，需要能够合并的Trigger和Windows Function，比如ReduceFunction，AggregateFunction，ProcessWindowFunction等。
        WindowedStream<Tuple3<Long, String, Integer>, String, TimeWindow> window = keyedStream.window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple3<Long, String, Integer>>() {
            @Override
            public long extract(Tuple3<Long, String, Integer> element) {
                // 完成动态调整session grap的具体逻辑
                return element.f0;
            }
        }));

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> result = window.sum(2);

        result.print();
        environment.execute();
    }
}
