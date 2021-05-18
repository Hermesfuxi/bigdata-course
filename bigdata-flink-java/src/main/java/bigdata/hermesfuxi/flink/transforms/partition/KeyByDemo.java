package bigdata.hermesfuxi.flink.transforms.partition;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class KeyByDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 22222);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStreamSource<String> source1 = env.socketTextStream("hadoop-slave1", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneOperator = source1.flatMap(
                        // 写类型
                        (String line, Collector<Tuple2<String, Integer>> collector) ->
                                // 方法向下转
//                        (FlatMapFunction<String, Tuple2<String, Integer>>) (line, collector) ->
                                Arrays.stream(line.split("\\s+")).forEach(str -> collector.collect(Tuple2.of(str, 1)))
                )
                .returns(Types.TUPLE(Types.STRING, Types.INT));


        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndOneOperator.keyBy(tuple2 -> tuple2.f0);

//        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(value);
            }
        }).keyBy(tuple2 -> tuple2.f0).process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(value);
            }
        });

        result.print();

        env.execute(Thread.currentThread().getStackTrace()[0].getFileName());
    }
}
