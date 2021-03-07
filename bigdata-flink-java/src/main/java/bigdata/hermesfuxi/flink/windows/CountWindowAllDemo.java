package bigdata.hermesfuxi.flink.windows;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * @author Hermesfuxi
 * desc:
 */
public class CountWindowAllDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 11111);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        DataStreamSource<String> lines = environment.socketTextStream("hadoop-slave3", 8888);
        SingleOutputStreamOperator<Integer> integerOperator = lines.map(Integer::parseInt);
        AllWindowedStream<Integer, GlobalWindow> countWindowAll = integerOperator.countWindowAll(5);
        SingleOutputStreamOperator<Integer> result = countWindowAll.sum(0);
        result.print();
        environment.execute();
    }
}
