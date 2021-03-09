package bigdata.hermesfuxi.flink.states.keyed;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class MapStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //CheckPointing 默认重启策略为 固定延迟无限重启
        environment.enableCheckpointing(5000);
        environment.setStateBackend(new FsStateBackend("file:///D:\\WorkSpaces\\IdeaProjects\\bigdata-course\\.ck\\flink"));

        DataStreamSource<String> source = environment.socketTextStream("hadoop-slave3", 8888);

        //河北省,石家庄,1
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> flatMapOperator = source.flatMap(new FlatMapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                if (value.contains("error")) {
                    throw new Exception("输入数据有误");
                } else {
                    String[] split = value.split(",");
                    out.collect(Tuple3.of(split[0], split[1], Integer.valueOf(split[2])));
                }
            }
        });

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> result = flatMapOperator
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple3<String, String, Integer>, Tuple3<String, String, Integer>>() {
                    private transient MapState<String, Integer> mapState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor mapStateDescriptor = new MapStateDescriptor<>("mapStateDemo", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<Integer>() {}));
                        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
                    }

                    @Override
                    public void processElement(Tuple3<String, String, Integer> value, Context ctx, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                        Integer historyCount = mapState.get(value.f1);
                        if (historyCount == null) {
                            historyCount = 0;
                        }
                        Integer totalCount = value.f2 + historyCount;
                        mapState.put(value.f1, totalCount);
                        value.f2 = totalCount;
                        out.collect(value);
                    }
                });
        result.print();

        environment.execute();
    }


}
