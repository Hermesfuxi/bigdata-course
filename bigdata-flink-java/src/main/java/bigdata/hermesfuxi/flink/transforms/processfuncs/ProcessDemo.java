package bigdata.hermesfuxi.flink.transforms.processfuncs;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;

public class ProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("hadoop-slave3", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = source.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split("\\s+");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(t -> t.f0).process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            private transient ValueState<Integer> valueState;
            private String key;
            private int index;

            @Override
            public void open(Configuration parameters) throws Exception {
                RuntimeContext runtimeContext = getRuntimeContext();
                ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("processDemo", Integer.class);
                valueState = runtimeContext.getState(descriptor);
                index = runtimeContext.getIndexOfThisSubtask();
            }

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                Integer historyCount = valueState.value();
                if (historyCount == null) {
                    historyCount = 0;
                }
                key = value.f0;
                Integer sum = historyCount + value.f1;
                valueState.update(sum);
                value.f1 = sum;
                out.collect(value);
            }
        });
        result.addSink(new PrintSinkFunction<>());

        env.execute();
    }
}
