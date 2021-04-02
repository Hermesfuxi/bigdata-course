package bigdata.hermesfuxi.flink.windows.evictors;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class CountEvictorDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop-slave1", 8888);
        socketTextStream.windowAll(GlobalWindows.create())
                .trigger(CountTrigger.of(3))
                // 驱逐器能够在触发器触发之后，窗口函数使用之前或之后从窗口中清除元素.在使用窗口函数之前被逐出的元素将不被处理
                .evictor(CountEvictor.of(2))
                .process(new ProcessAllWindowFunction<String, String, GlobalWindow>() {
                    @Override
                    public void process(Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        for (String element : elements) {
                            out.collect(element);
                        }
                    }
                })
                .print();

        env.execute();
    }


}
