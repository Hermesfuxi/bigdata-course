package bigdata.hermesfuxi.flink.sources;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class CustomSourceDemo2 {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 11111);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        int parallelism = environment.getParallelism();
        System.out.println("env并行度为：" + parallelism);

        DataStreamSource<String> dataStreamSource = environment.addSource(new MySource1());
        int sourceParallelism = dataStreamSource.getParallelism();
        System.out.println("Source的并行度为：" + sourceParallelism);

        dataStreamSource.print();
        environment.execute();
    }

    private static class MySource1 implements SourceFunction<String> {
        private boolean flag = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (flag) {
                String id = UUID.randomUUID().toString();
                ctx.collect(id);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            flag = false;
            System.out.println("cancel invoked");
        }
    }
}
