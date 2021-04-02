package bigdata.hermesfuxi.flink.windows.time;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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
public class ProcessingTimeWindowAllDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> source = environment.socketTextStream("hadoop-slave3", 8888);
        SingleOutputStreamOperator<Integer> mapOperator = source.map(Integer::valueOf);

//        AllWindowedStream<Integer, TimeWindow> window = mapOperator.timeWindowAll(Time.seconds(5));
        AllWindowedStream<Integer, TimeWindow> window = mapOperator.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)));
//        AllWindowedStream<Integer, TimeWindow> window = mapOperator.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)));
//        AllWindowedStream<Integer, TimeWindow> window = mapOperator.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));

        SingleOutputStreamOperator<Integer> result = window.sum(0);
        result.print();
        environment.execute();
    }
}
