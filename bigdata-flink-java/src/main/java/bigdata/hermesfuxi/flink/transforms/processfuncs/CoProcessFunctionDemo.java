package bigdata.hermesfuxi.flink.transforms.processfuncs;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class CoProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream1 = env.socketTextStream("hadoop-slave1", 8888);
        KeyedStream<Tuple2<String, Integer>, String> keyedStream1 = stream1.flatMap(new MyFlatMapFunction()).keyBy(t -> t.f0);

        DataStreamSource<String> stream2 = env.socketTextStream("hadoop-slave2", 8888);
        KeyedStream<Tuple2<String, Integer>, String> keyedStream2 = stream2.flatMap(new MyFlatMapFunction()).keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = keyedStream1.connect(keyedStream2).process(new CoProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

            }

            @Override
            public void processElement2(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

            }
        });

        env.execute();
    }

    private static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>>{
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split("\\s+");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }
    }
}
