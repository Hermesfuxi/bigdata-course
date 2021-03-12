package bigdata.hermesfuxi.flink.transforms;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 22222);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStreamSource<String> source1 = env.socketTextStream("hadoop-slave1", 8888);
        DataStreamSource<String> source2 = env.socketTextStream("hadoop-slave2", 8888);
        SingleOutputStreamOperator<Integer> streamOperator = source2.map(Integer::valueOf);

        SingleOutputStreamOperator<String> result = source1.connect(streamOperator).map(new CoMapFunction<String, Integer, String>() {

            @Override
            public String map1(String value) throws Exception {
                return value;
            }

            @Override
            public String map2(Integer value) throws Exception {
                return value.toString();
            }
        });
        result.print();

        env.execute();
    }
}
