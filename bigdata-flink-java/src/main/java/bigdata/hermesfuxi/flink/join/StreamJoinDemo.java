package bigdata.hermesfuxi.flink.join;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.Collector;

public class StreamJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // a,2,4
        // b,3,6
        // b,4,7
        DataStreamSource<String> source1 = env.socketTextStream("hadoop-slave1", 8888);
        SingleOutputStreamOperator<Tuple3<String, String, String>> stream1 = source1.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });

        // a c d f e g h
        // b e f
        // a c b
        DataStreamSource<String> source2 = env.socketTextStream("hadoop-slave2", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> stream2 = source2.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split("\\s+");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }

        });

        DataStream<Tuple4<String, String, String, Integer>> result = stream1.join(stream2)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(2))
                .apply(new JoinFunction<Tuple3<String, String, String>, Tuple2<String, Integer>, Tuple4<String, String, String, Integer>>() {
                    @Override
                    public Tuple4<String, String, String, Integer> join(Tuple3<String, String, String> first, Tuple2<String, Integer> second) throws Exception {
                        return Tuple4.of(first.f0, first.f1, first.f2, second.f1);
                    }
                });

        result.print();

        env.execute();
    }
}
