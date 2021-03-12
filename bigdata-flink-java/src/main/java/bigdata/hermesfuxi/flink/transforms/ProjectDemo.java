package bigdata.hermesfuxi.flink.transforms;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Tuple的简易取值操作：可用 map 实现
 */
public class ProjectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("hadoop-slave3", 8888);
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> dataStream = source.flatMap(new FlatMapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                String[] fields = value.split(",");
                out.collect(Tuple3.of(fields[0], fields[1], Integer.valueOf(fields[2])));
            }
        });
        dataStream.print();
        dataStream.project(1).project(0).print();
        dataStream.project(0, 2).print();

        env.execute();
    }
}
