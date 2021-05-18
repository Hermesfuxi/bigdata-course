package bigdata.hermesfuxi.flink.windows.join;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
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
        env.setParallelism(1);

/* 数据源1：
a,2,4
b,3,6
b,4,7
*/
        DataStreamSource<String> source1 = env.socketTextStream("hadoop-slave1", 8888);
        SingleOutputStreamOperator<Tuple3<String, String, String>> stream1 = source1.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });

/* 数据源2：
a c d
f e g h
b e f
a c b
 */
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

        // join = InnerJoin
        DataStream<Tuple4<String, String, String, Integer>> joinResult = stream1.join(stream2)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(2))
                .apply(new JoinFunction<Tuple3<String, String, String>, Tuple2<String, Integer>, Tuple4<String, String, String, Integer>>() {
                    @Override
                    public Tuple4<String, String, String, Integer> join(Tuple3<String, String, String> left, Tuple2<String, Integer> right) throws Exception {
                        return Tuple4.of(left.f0, left.f1, left.f2, right.f1);
                    }
                });
        joinResult.print();

        // leftJoin 由 coGroup 实现，rightJoin 相反
        DataStream<Tuple4<String, String, String, Integer>> rightJoinResult = stream1.coGroup(stream2)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .window(GlobalWindows.create())
                .apply(new RichCoGroupFunction<Tuple3<String, String, String>, Tuple2<String, Integer>, Tuple4<String, String, String, Integer>>() {
                    // 在一个流/数据集中没有找到与另一个匹配的数据还是会输出
                    @Override
                    public void coGroup(Iterable<Tuple3<String, String, String>> left, Iterable<Tuple2<String, Integer>> right, Collector<Tuple4<String, String, String, Integer>> out) throws Exception {
//                        boolean hadLeftElements = false;

                        for (Tuple3<String, String, String> leftElement : left) {
//                            hadLeftElements = true;

                            boolean hadRightElements = false;
                            for (Tuple2<String, Integer> rightElement : right) {
                                // 这里面的都是 InnerJoin
                                out.collect(Tuple4.of(leftElement.f0, leftElement.f1, leftElement.f2, rightElement.f1));
                                hadRightElements = true;
                            }

                            // 若right为空，join不上，则需给left赋空值，如果没有这个就是InnerJoin，而 rightJoin 实现刚好相反
                            if (!hadRightElements) {
                                out.collect(Tuple4.of(leftElement.f0, leftElement.f1, leftElement.f2, null));
                            }
                        }

                        // full join 的实现
//                        if (!hadLeftElements) {
//                            for (Tuple2<String, Integer> rightElement : right) {
//                                // 这里面的都是 InnerJoin
//                                out.collect(Tuple4.of(rightElement.f0, null, null, rightElement.f1));
//                            }
//                        }
                    }
                });

        rightJoinResult.print();

        env.execute();
    }
}
