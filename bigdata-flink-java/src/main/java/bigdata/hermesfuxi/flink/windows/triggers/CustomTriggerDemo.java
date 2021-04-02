package bigdata.hermesfuxi.flink.windows.triggers;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class CustomTriggerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop-slave1", 8888);
        socketTextStream.windowAll(GlobalWindows.create())
                .trigger(new MyTrigger())
                .process(new ProcessAllWindowFunction<String, String, GlobalWindow>() {
                    @Override
                    public void process(Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        StringBuilder str = new StringBuilder(" ");
                        for (String element : elements) {
                            str.append(element).append(" ");
                        }
                        out.collect(str.toString());
                    }
                }).print();

        env.execute();
    }

    static class MyTrigger extends Trigger<String, GlobalWindow>{

        // 在每个element 添加到window的时候会被回调
        @Override
        public TriggerResult onElement(String element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
            return null;
        }

        @Override
        public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            return null;
        }

        @Override
        public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            return null;
        }

        // 用于清除TriggerContext中存储的相关state
        @Override
        public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
            // TriggerContext定义了注册及删除 EventTimeTimer、ProcessingTimeTimer 方法，
            // 还定义了 getCurrentProcessingTime、getMetricGroup、getCurrentWatermark、
            // getPartitionedState、getKeyValueState、getKeyValueState 方法


        }

        // 用于标识是否支持 trigger state的合并，默认返回false
        @Override
        public boolean canMerge() {
            return super.canMerge();
        }

        // 在多个window合并的时候会被触发
        @Override
        public void onMerge(GlobalWindow window, OnMergeContext ctx) throws Exception {
            // OnMergeContext继承了TriggerContext，它多定义了mergePartitionedState方法
            super.onMerge(window, ctx);
        }
    }
}
