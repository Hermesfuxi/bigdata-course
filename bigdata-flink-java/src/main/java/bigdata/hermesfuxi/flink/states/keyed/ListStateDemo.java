package bigdata.hermesfuxi.flink.states.keyed;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class ListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //CheckPointing 默认重启策略为 固定延迟无限重启
        environment.enableCheckpointing(5000);
        environment.setStateBackend(new FsStateBackend("file:///D:\\WorkSpaces\\IdeaProjects\\bigdata-course\\.ck\\flink"));

        DataStreamSource<String> source = environment.socketTextStream("hadoop-slave3", 8888);

        SingleOutputStreamOperator<Tuple2<String, String>> flatMapOperator = source.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
                if (value.contains("error")) {
                    throw new Exception("输入数据有误");
                }else{
                    String[] split = value.split(",");
                    out.collect(Tuple2.of(split[0], split[1]));
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, List<String>>> result = flatMapOperator
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, List<String>>>() {
                    private transient ListState<String> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<String>("listStateDemo", String.class));
                    }

                    @Override
                    public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, List<String>>> out) throws Exception {
//                        Iterable<String> strings = listState.get();
//                        List<String> list = new ArrayList<String>();
//                        for (String str : strings) {
//                            list.add(str);
//                        }
//
//                        String uid = value.f0;
//                        String event = value.f1;
//                        list.add(event);
//                        listState.update(list);
//                        out.collect(Tuple2.of(uid, list));

                        listState.add(value.f1);
                        out.collect(Tuple2.of(value.f0, Lists.newArrayList(listState.get())));
                    }

                });
        result.print();

        environment.execute();
    }


}
