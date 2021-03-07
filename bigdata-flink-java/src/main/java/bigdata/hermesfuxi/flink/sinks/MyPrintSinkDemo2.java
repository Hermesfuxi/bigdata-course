package bigdata.hermesfuxi.flink.sinks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class MyPrintSinkDemo2 {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 11111);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> textStream = environment.socketTextStream("hadoop-slave3", 8888);


        textStream.addSink(new MyPrintSink()).name("MyPrintSinkDemo2");

        environment.execute();
    }

    private static class MyPrintSink extends RichSinkFunction<String> {
        private int index;
        @Override
        public void open(Configuration parameters) throws Exception {
            index = 1 + getRuntimeContext().getIndexOfThisSubtask();
        }

        @Override
        public void invoke(String value, Context context) throws Exception {

            System.out.println(index + "> " + value);
        }

        @Override
        public void close() throws Exception {
            System.out.println("MyPrintSink " + index + " is closed");
        }
    }
}
