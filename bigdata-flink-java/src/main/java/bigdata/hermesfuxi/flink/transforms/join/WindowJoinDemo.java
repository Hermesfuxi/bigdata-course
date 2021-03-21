package bigdata.hermesfuxi.flink.transforms.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowJoinDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source1 = env.socketTextStream("hadoop-slave1", 8888);
        DataStreamSource<String> source2 = env.socketTextStream("hadoop-slave2", 8888);

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> table1 = source1.map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
            @Override
            public Tuple3<String, Long, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], Long.valueOf(fields[1]), Integer.valueOf(fields[2]));
            }
        });

        SingleOutputStreamOperator<Tuple3<Integer, String, Long>> table2 = source2.map(new MapFunction<String, Tuple3<Integer, String, Long>>() {
            @Override
            public Tuple3<Integer, String, Long> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(Integer.valueOf(fields[0]), fields[1], Long.valueOf(fields[2]));
            }
        });

        // innerJoin
        table1.join(table2).where(t -> t.f0).equalTo(t -> t.f1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new RichJoinFunction<Tuple3<String, Long, Integer>, Tuple3<Integer, String, Long>, Tuple5<String, Long, Integer, Integer, Long>>() {
                    @Override
                    public Tuple5<String, Long, Integer, Integer, Long> join(Tuple3<String, Long, Integer> first, Tuple3<Integer, String, Long> second) throws Exception {
                        return Tuple5.of(first.f0, first.f1, first.f2, second.f0, second.f2);
                    }
                })
                .print();
        env.execute();
    }
}
