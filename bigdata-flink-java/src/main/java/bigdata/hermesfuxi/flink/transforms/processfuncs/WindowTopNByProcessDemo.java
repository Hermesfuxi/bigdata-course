package bigdata.hermesfuxi.flink.transforms.processfuncs;

import org.apache.commons.collections.list.TreeList;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Collector;

public class WindowTopNByProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        // spark,1
        DataStreamSource<String> source = env.socketTextStream("hadoop-slave3", 8888);
        source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], Integer.valueOf(fields[1]));
            }
        }).keyBy(t -> t.f0).process(new MyWindowTopNFunction(5000L, 3)).print();

        env.execute();
    }

    private static class MyWindowTopNFunction extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>> {
        private Long windowTime;
        private Integer topN;
        private transient ListState<Tuple2<String, Integer>> listState;

        public MyWindowTopNFunction(Long windowTime, Integer topN) {
            this.windowTime = windowTime;
            this.topN = topN;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<Tuple2<String, Integer>> descriptor = new ListStateDescriptor<>(
                    "WindowTopNByProcess",
                    TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                    })
            );
            listState = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            //输入一条数据，先攒起来，不输出
            listState.add(value);

            // 时间处理
            System.out.println("key:" + value.f0);

            //获取当前的系统时间
            long processingTime = ctx.timerService().currentProcessingTime();
            long triggerTime = processingTime - (processingTime % windowTime) + windowTime;
            ctx.timerService().registerProcessingTimeTimer(triggerTime);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            Iterable<Tuple2<String, Integer>> tuple2s = listState.get();
            CollectionUtil.iterableToList(tuple2s).stream().sorted((o1, o2) -> o2.f1 - o1.f1).limit(topN).forEachOrdered(out::collect);
        }
    }
}
