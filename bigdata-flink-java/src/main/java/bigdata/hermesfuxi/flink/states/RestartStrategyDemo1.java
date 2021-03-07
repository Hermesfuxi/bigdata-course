package bigdata.hermesfuxi.flink.states;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Hermesfuxi
 * desc: 从集合中创建一个数据流，集合中所有元素的类型是一致的。
 */
public class RestartStrategyDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //CheckPointing 默认重启策略为 固定延迟无限重启
        environment.enableCheckpointing(5000);

        // —— 重启策略 ——
        // 1、无重启策略(No restart strategy)
//        environment.setRestartStrategy(RestartStrategies.noRestart());

        // 2、固定延迟重启策略(Fixed Delay Restart Strategy)
        // fixed-delay.attempts 任务失败后重启次数 ; fixed-delay.delay  任务失败几秒后需要开始执行重启操作
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));
//        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000)); // 默认毫秒级别

        // 3、失败率重启策略：指定周期内重启
        // failure-rate.max-failures-per-interval 任务认定为失败之前，最大的重启次数
        // failure-rate.failure-rate-interval	 计算失败率的时间间隔
        // failure-rate.delay	 两次尝试重启之间时间间隔
//        environment.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(1), Time.seconds(5)));

        // 4、 集群回退重新启动
//        environment.setRestartStrategy(RestartStrategies.fallBackRestart());

        DataStreamSource<String> source = environment.socketTextStream("hadoop-slave3", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapOperator = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split("\\s+");
                for (String str : split) {
                    if (str.contains("error")) {
                        throw new Exception("输入数据有误");
                    }else{
                        out.collect(Tuple2.of(str, 1));
                    }
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = flatMapOperator.keyBy(t -> t.f0).sum(1);
        result.print();

        environment.execute();
    }
}
