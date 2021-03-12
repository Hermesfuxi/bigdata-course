package bigdata.hermesfuxi.flink.transforms;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Iterate迭代流式计算
 */
public class IterateDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 22222);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> source = environment.socketTextStream("hadoop-slave3", 8888);
        SingleOutputStreamOperator<Long> numbers = source.map(Long::parseLong);
        IterativeStream<Long> numIterate = numbers.iterate();

        // 10 -8 9
        SingleOutputStreamOperator<Long> iterationBody = numIterate.map(t -> t - 2);

        SingleOutputStreamOperator<Long> feedback = iterationBody.filter(t -> t > 0);
        //只要满足value > 0的条件，就会形成一个回路，重新的迭代，即将前面的输出作为输入，在进行一次应用更新模型，即输入数据的处理逻辑
        numIterate.closeWith(feedback);

        //不满足迭代条件的最后要输出
        SingleOutputStreamOperator<Long> outData = iterationBody.filter(t -> t <= 0);

        outData.print();
        // 3> 0
        // 4> -10
        // 5> -1

        environment.execute();
    }
}
