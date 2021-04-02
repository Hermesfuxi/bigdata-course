package bigdata.hermesfuxi.flink.windows.evictors;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class TimeEvictorDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 22222);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop-slave1", 8888);
        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> streamOperator = socketTextStream.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(Long.parseLong(fields[0]), fields[1], Integer.valueOf(fields[2]));
            }
        });

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> streamAndWatermarks = streamOperator.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<Long, String, Integer>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((a, b) -> a.f0)
        );

        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator1 = streamAndWatermarks.map(new MapFunction<Tuple3<Long, String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple3<Long, String, Integer> value) throws Exception {
                return Tuple2.of(value.f1, value.f2);
            }
        });

        streamOperator1.keyBy(t -> t.f0)

                .window(TumblingEventTimeWindows.of(Time.milliseconds(5)))
                .evictor(TimeEvictor.of(Time.milliseconds(2)))

                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Integer count = 0;
                        for (Tuple2<String, Integer> element : elements) {
                            count += element.f1;
                        }
                        out.collect(Tuple2.of(key, count));
                    }
                })
                .print();
        env.execute();
    }
}
