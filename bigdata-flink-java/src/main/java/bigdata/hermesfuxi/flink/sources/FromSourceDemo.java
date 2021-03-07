package bigdata.hermesfuxi.flink.sources;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.NumberSequenceIterator;

import java.util.Arrays;
import java.util.List;

/**
 * @author Hermesfuxi
 * desc: 从集合中创建一个数据流，集合中所有元素的类型是一致的。
 */
public class FromSourceDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 11111);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        int parallelism = environment.getParallelism();
        System.out.println("env并行度为：" + parallelism);

        DataStreamSource<String> dataStreamSource1 = environment.fromElements("AAA", "BBB", "VVV");
        dataStreamSource1.print();
        //        int sourceParallelism = dataStreamSource1.getParallelism();
//        System.out.println("Source1的并行度为：" + sourceParallelism);

        List<String> array = Arrays.asList("a", "b", "c", "d");
        DataStreamSource<String> dataStreamSource2 = environment.fromCollection(array);
        dataStreamSource2.print();
        //        int sourceParallelism = dataStreamSource2.getParallelism();
//        System.out.println("Source2的并行度为：" + sourceParallelism);

        DataStreamSource<Long> dataStreamSource3 = environment.fromParallelCollection(new NumberSequenceIterator(1, 100), Long.class);
        dataStreamSource3.print();
//        int sourceParallelism = dataStreamSource3.getParallelism();
//        System.out.println("Source3的并行度为：" + sourceParallelism);

        environment.execute();
    }
}
