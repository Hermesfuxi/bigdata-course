package bigdata.hermesfuxi.flink.sinks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WriteAsTextSinkDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 11111);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        DataStreamSource<String> source = environment.socketTextStream("hadoop-slave3", 8888);
        source.name("WriteAsTextSource");

        source.writeAsText("data/out/flink/nc1").name("WriteAsTextSink");
        environment.execute("WriteAsTextSinkDemo");
    }
}
