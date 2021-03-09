package bigdata.hermesfuxi.flink.states;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Flink状态生存时间（State TTL）机制: 具体见 https://blog.csdn.net/nazeniwaresakini/article/details/106094778
 */
public class StateTtlConfigDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //CheckPointing 默认重启策略为 固定延迟无限重启
        environment.enableCheckpointing(5000);
        environment.setStateBackend(new FsStateBackend("file:///D:\\WorkSpaces\\IdeaProjects\\bigdata-course\\.ck\\flink"));

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

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = flatMapOperator
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    private transient ValueState<Integer> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("valueStateWC", Integer.class);
                        //定义一个状态TTLConfig: 一旦设置了 TTL，那么如果上次访问的时间戳 + TTL 超过了当前时间，则表明状态过期了
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(10))

//                                // 过期对象的清理策略
//                                // 全量快照时清理
//                                .cleanupFullSnapshot()
//                                // 增量清理: 默认在每次访问状态时进行清理（cleanupSize指定每次触发清理时检查的状态条数， runCleanupForEveryRecord设为true可以附加在每次写入/删除时清理）
//                                .cleanupIncrementally(10, true)
//                                // 当RocksDB做compaction操作时，通过Flink定制的过滤器（FlinkCompactionFilter）过滤掉过期状态数据
//                                // 参数queryTimeAfterNumEntries用于指定在写入多少条状态数据后，通过状态时间戳来判断是否过期
//                                .cleanupInRocksdbCompactFilter(1000)
//                                // 禁用默认后台清理
//                                .disableCleanupInBackground()


                                // 表示 State TTL 功能所适用的时间模式
//                                .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)
//                                .useProcessingTime() // 同上

                                //状态时间戳的更新的时机: Disabled-不更新 OnCreateAndWrite-状态创建或写入时 OnReadAndWrite: 状态创建、写入、读取时
//                                .setUpdateType(StateTtlConfig.UpdateType.Disabled)
//                                .updateTtlOnCreateAndWrite()
                                .updateTtlOnReadAndWrite()

                                // 对已过期但还未被清理掉的状态如何处理：
                                // ReturnExpiredIfNotCleanedUp - 即使这个状态的时间戳表明它已经过期了，但是只要还未被真正清理掉，就会被返回给调用方；
                                // NeverReturnExpired - 那么一旦这个状态过期了，那么永远不会被返回给调用方，只会返回空状态，避免了过期状态带来的干扰。
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//                                .neverReturnExpired() // 同上
//                                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
//                                .returnExpiredIfNotCleanedUp() // 同上

                                .build();

                        descriptor.enableTimeToLive(ttlConfig);
                        valueState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Integer historyValue = valueState.value();
                        Integer currentValue = value.f1;
                        if(historyValue == null){
                            historyValue = 0;
                        }
                        Integer newValue = currentValue + historyValue;
                        valueState.update(newValue);

                        value.f1 = newValue;
                        out.collect(value);
                    }
                });
        result.print();

        environment.execute();
    }
}
