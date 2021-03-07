package bigdata.hermesfuxi.flink.sources;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;

/**
 * @author Hermesfuxi
 * desc: 自定义 source
 */
public class CustomSourceDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 11111);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        int parallelism = environment.getParallelism();
        System.out.println("env并行度为：" + parallelism);

        DataStreamSource<String> dataStreamSource = environment.addSource(new SourceFunction<String>() {
            // 运行时调用一次
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                List<String> words = Arrays.asList("spark", "hadoop", "flink", "spark", "hadoop");
                for (String word : words) {
                    ctx.collect(word);
                }
            }
            // 取消任务时调用一次
            @Override
            public void cancel() {
                System.out.println("cancel invoked");
            }
        });

        int sourceParallelism = dataStreamSource.getParallelism();
        System.out.println("Source的并行度为：" + sourceParallelism);

        dataStreamSource.print();
        environment.execute();
    }
}
