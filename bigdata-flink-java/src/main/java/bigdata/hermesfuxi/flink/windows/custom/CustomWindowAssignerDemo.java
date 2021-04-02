package bigdata.hermesfuxi.flink.windows.custom;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * @author Hermesfuxi
 * 实现根据周或月的窗口划分窗口
 * 比如按照 周日的 00:00:00 到 下一个周六 23:59:59
 * 或者 每个月第一天的 00:00:00 到 最后一天的 23:59:59
 */
public class CustomWindowAssignerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop-slave1", 8888);
        SingleOutputStreamOperator<TopInput> streamOperator = socketTextStream.process(new ProcessFunction<String, TopInput>() {
            @Override
            public void processElement(String value, Context ctx, Collector<TopInput> out) throws Exception {
                TopInput topInput = JSONObject.parseObject(value, TopInput.class);
                if (topInput != null) {
                    out.collect(topInput);
                }
            }
        });

        streamOperator.keyBy(TopInput::getWeekTag)
                // 实现多重功能窗口
                .window(new CustomWindowAssigner<>("week"))
                .process(new ProcessWindowFunction<TopInput, Tuple2<String, Integer>, String, TimeWindow>() {
                    private Integer count;

                    @Override
                    public void process(String s, Context context, Iterable<TopInput> elements, Collector<Tuple2<String, Integer>> out) throws Exception {

                        for (TopInput element : elements) {
                            // 去重
                            // element.getUid();
                            count ++;
                        }

                        out.collect(Tuple2.of(s, count));
                    }
                }).print();

        env.execute();
    }

}

/**
 * 实现参考了 TumblingEventTimeWindows
 *
 * T 需要划分窗口的数据类型(输入类型)
 */
class CustomWindowAssigner<T> extends WindowAssigner<T, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private String tag;

    public CustomWindowAssigner() {
    }

    public CustomWindowAssigner(String tag) {
        this.tag = tag;
    }

    //窗口分配的主要方法，需要为每一个元素指定所属的分区
    @Override
    public Collection<TimeWindow> assignWindows(T element, long timestamp, WindowAssignerContext context) {
        Tuple2<Long, Long> tuple2 = null;
        if("week".equals(tag)){
            tuple2 = getTimestampFromWeek(timestamp);
        }else if("month".equals(tag)){
            tuple2 = getTimestampFromMon(timestamp);
        }
        // 分配窗口
        assert tuple2 != null;
        return Collections.singletonList(new TimeWindow(tuple2.f0, tuple2.f1));
    }

    @Override
    public Trigger<T, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return new CustomEventTimeTrigger<T>();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    // 是否使用事件时间
    @Override
    public boolean isEventTime() {
        return true;
    }

    /**
     * 获取指定时间戳当月时间戳范围
     * eg:2020-03-12 11:35:13 (timestamp=1583984113960l)
     * 结果为：(1582992000000,1585670399999)=>(2020-03-01 00:00:00,2020-03-31 23:59:59)
     */
    public Tuple2<Long, Long> getTimestampFromMon(Long timestamp){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date(timestamp));

        //设置为当月第一天
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        //将小时至0
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        //将分钟至0
        calendar.set(Calendar.MINUTE, 0);
        //将秒至0
        calendar.set(Calendar.SECOND,0);
        //将毫秒至000
        calendar.set(Calendar.MILLISECOND, 0);
        // 获取本月最开始的时间戳
        long startTimestamp = calendar.getTimeInMillis();

        //设置为当月最后一天
        calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
        //将小时至23
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        //将分钟至59
        calendar.set(Calendar.MINUTE, 59);
        //将秒至59
        calendar.set(Calendar.SECOND,59);
        //将毫秒至999
        calendar.set(Calendar.MILLISECOND, 999);
        // 获取本月最后一天的时间戳
        long endTimestamp = calendar.getTimeInMillis();

        return Tuple2.of(startTimestamp, endTimestamp);
    }

    /**
     * 获取指定时间戳的当周时间戳范围（从周日开始）
     * eg:2020-03-12 11:35:13 (timestamp=1583984113960l)
     * 结果为：(1582992000000,1585670399999)=>(2020-03-01 00:00:00,2020-03-31 23:59:59)
     */
    public Tuple2<Long, Long> getTimestampFromWeek(Long timestamp){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date(timestamp));
        // 设置周日为首日  默认值，一般不用设置
//        calendar.setFirstDayOfWeek(Calendar.SUNDAY)
        //设置为当周第一天
        calendar.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
        //将小时至0
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        //将分钟至0
        calendar.set(Calendar.MINUTE, 0);
        //将秒至0
        calendar.set(Calendar.SECOND,0);
        //将毫秒至000
        calendar.set(Calendar.MILLISECOND, 0);
        // 获取本月最开始的时间戳
        long startTimestamp = calendar.getTimeInMillis();

        //设置为当周最后一天
        calendar.set(Calendar.DAY_OF_WEEK, Calendar.SATURDAY);
        //将小时至23
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        //将分钟至59
        calendar.set(Calendar.MINUTE, 59);
        //将秒至59
        calendar.set(Calendar.SECOND,59);
        //将毫秒至999
        calendar.set(Calendar.MILLISECOND, 999);
        // 获取本月最后一天的时间戳
        long endTimestamp = calendar.getTimeInMillis();

        return Tuple2.of(startTimestamp, endTimestamp);
    }
}

class CustomEventTimeTrigger<T> extends Trigger<T, TimeWindow>{
    private long maxCount = 1000;

    public CustomEventTimeTrigger() {
    }

    public CustomEventTimeTrigger(Long maxCount) {
        this.maxCount = maxCount;
    }

    private final ReducingStateDescriptor<Long> stateDesc =
            new ReducingStateDescriptor<>("count", new Sum(), LongSerializer.INSTANCE);

    private static class Sum implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
        }
    }

    @Override
    public TriggerResult onElement(T element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        ReducingState<Long> countState = ctx.getPartitionedState(stateDesc);
        countState.add(1L);
        if (countState.get() >= maxCount) {
            countState.clear();
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }
}