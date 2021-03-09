package bigdata.hermesfuxi.flink.states.keyed;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ReducingStateDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

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
                    private transient ReducingState<Integer> reducingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>("ReducingStateWC", new ReduceFunction<Integer>() {
                            @Override
                            public Integer reduce(Integer value1, Integer value2) throws Exception {
                                return value1 + value2;
                            }
                        },
                        Integer.class));
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Integer historyValue = reducingState.get();
                        if(historyValue == null){
                            historyValue = 0;
                        }
                        Integer currentValue = value.f1;
                        reducingState.add(currentValue);

                        value.f1 = currentValue + historyValue;
                        out.collect(value);
                    }
                });
        result.print();

        environment.execute();
    }
}
