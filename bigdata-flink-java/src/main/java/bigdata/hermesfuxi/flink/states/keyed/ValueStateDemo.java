package bigdata.hermesfuxi.flink.states.keyed;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ValueStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //CheckPointing 默认重启策略为 固定延迟无限重启
        environment.enableCheckpointing(5000);
        environment.setStateBackend(new FsStateBackend("file:///D:\\WorkSpaces\\IdeaProjects\\bigdata-course\\.ck\\flink"));

        DataStreamSource<String> source = environment.socketTextStream("hadoop-slave3", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapOperator = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split("\\s+");
                for (String str : split) {
                    if (str.contains("error")) {
                        throw new Exception("输入数据有误");
                    }else{
                        out.collect(Tuple2.of(str, 1));
                    }
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = flatMapOperator
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    private transient ValueState<Integer> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("valueStateWC", Integer.class));
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Integer historyValue = valueState.value();
                        Integer currentValue = value.f1;
                        if(historyValue == null){
                            historyValue = 0;
                        }
                        Integer newValue = currentValue + historyValue;
                        valueState.update(newValue);

                        value.f1 = newValue;
                        out.collect(value);
                    }
                });
        result.print();

        environment.execute();
    }


}
