package bigdata.hermesfuxi.flink.sinks;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class writeToSocketSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = environment.socketTextStream("hadoop-slave3", 8888);

        streamSource.writeToSocket("hadoop-slave3", 8888, new SimpleStringSchema());

        JobExecutionResult execute = environment.execute(Thread.currentThread().getStackTrace()[0].getClassName());

        long netRuntime = execute.getNetRuntime();

        System.out.println(netRuntime);
    }
}
