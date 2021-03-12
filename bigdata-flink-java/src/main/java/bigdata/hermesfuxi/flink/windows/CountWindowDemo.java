package bigdata.hermesfuxi.flink.windows;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

public class CountWindowDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 22222);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        DataStreamSource<String> lines = environment.socketTextStream("hadoop-slave3", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = lines.map(new RichMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] strings = value.split(",");
                return Tuple2.of(strings[0], Integer.valueOf(strings[1]));
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = streamOperator.keyBy(t -> t.f0);
        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> window = keyedStream.countWindow(5);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = window.sum(0);
        result.print();
        environment.execute();
    }
}
