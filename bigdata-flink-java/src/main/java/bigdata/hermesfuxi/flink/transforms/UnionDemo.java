package bigdata.hermesfuxi.flink.transforms;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Connect与 Union 区别：
 * 1． Union之前两个流的类型必须是一样，Connect可以不一样，在之后的coMap中再去调整成为一样的。
 * 2. Connect只能操作两个流，Union可以操作多个。
 * 3. Union只是单纯的将结果聚拢（将多流汇成一流），Connect可视为 join
 */
public class UnionDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 22222);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStreamSource<String> source1 = env.socketTextStream("hadoop-slave1", 8888);
        DataStreamSource<String> source2 = env.socketTextStream("hadoop-slave2", 8888);
        DataStreamSource<String> source3 = env.socketTextStream("hadoop-slave3", 8888);

        DataStream<String> union = source1.union(source2).union(source3);

        union.print();

        env.execute();
    }
}
