package bigdata.hermesfuxi.flink.sinks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class MyPrintSinkDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 22222);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> textStream = environment.socketTextStream("hadoop-slave3", 8888);

//        textStream.addSink(new SinkFunction<String>() {
//            @Override
//            public void invoke(String value, Context context) throws Exception {
//                System.out.println(value);
//            }
//        }).name("MyPrintSinkDemo1");

        textStream.addSink(new MyPrintSink()).name("MyPrintSinkDemo1");

        environment.execute();
    }

    private static class MyPrintSink implements SinkFunction<String> {
        @Override
        public void invoke(String value, Context context) throws Exception {
            /*
                -9223372036854775808
                1614587171443
                null
                lisa
             */
            long currentWatermark = context.currentWatermark();
            System.out.println(currentWatermark);

            long currentProcessingTime = context.currentProcessingTime();
            System.out.println(currentProcessingTime);

            Long timestamp = context.timestamp();
            System.out.println(timestamp);

            System.out.println(value);
        }
    }
}
