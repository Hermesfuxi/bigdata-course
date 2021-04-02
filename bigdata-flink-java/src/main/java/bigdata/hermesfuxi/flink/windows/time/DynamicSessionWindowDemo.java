package bigdata.hermesfuxi.flink.windows.time;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;

/*
数据的密度,随着时间的推移进行调整
 */
public class DynamicSessionWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop-slave3", 8888);

        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> streamOperator = socketTextStream.map(new MapFunction<String, Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], 1, Long.valueOf(fields[1]));
            }
        });

        streamOperator.keyBy(t -> t.f0).window(EventTimeSessionWindows.withDynamicGap(new DynamicSessionWindows())).sum(1).print();
        env.execute();
    }

    public static class DynamicSessionWindows implements SessionWindowTimeGapExtractor<Tuple3<String, Integer, Long>> {
        @Override
        public long extract(Tuple3<String, Integer, Long> element) {
            return element.f2;
        }
    }

}