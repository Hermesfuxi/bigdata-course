package bigdata.hermesfuxi.flink.sources;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.UUID;

public class CustomSourceDemo3 {
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

    private static class MySource1 extends RichParallelSourceFunction<String> {
        private boolean flag = true;

        @Override
        public void setRuntimeContext(RuntimeContext t) {
            super.setRuntimeContext(t);
        }

        @Override
        public RuntimeContext getRuntimeContext() {
            return super.getRuntimeContext();
        }

        @Override
        public IterationRuntimeContext getIterationRuntimeContext() {
            return super.getIterationRuntimeContext();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open method invoked");
        }

        @Override
        public void close() throws Exception {
            System.out.println("close method invoked");
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            //subTask的index是从0开始的
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();

            while (flag) {
                String id = UUID.randomUUID().toString();
                ctx.collect(indexOfThisSubtask + " ===> " + id);
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
