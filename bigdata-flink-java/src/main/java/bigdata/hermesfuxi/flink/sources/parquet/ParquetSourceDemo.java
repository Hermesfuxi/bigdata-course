package bigdata.hermesfuxi.flink.sources.parquet;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Hermesfuxi
 * desc: 从集合中创建一个数据流，集合中所有元素的类型是一致的。 都是有限的数据流
 */
public class ParquetSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> result = env.addSource(new ParquetSourceFunction2());

        result.print();

        env.execute();
    }
}
