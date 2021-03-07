package bigdata.hermesfuxi.flink.states;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class CheckPointDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //CheckPointing 默认重启策略为 固定延迟无限重启
        // 启动checkpoint, 并且设置多久进行一次checkpoint, 即两次checkpoint的时间间隔
        environment.enableCheckpointing(5000);

        CheckpointConfig checkpointConfig = environment.getCheckpointConfig();

        // 设置 checkpoint 语义: 一般使用 exactly_once (at_least_once 用于低延迟场景)
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        /*
            检查点之间的最短时间: 为确保流应用程序在检查点之间取得一些进展, 可以定义检查点之间需要经过多长时间。（可理解为保持最短车距）
            如果将此值设置为 500, 则无论Checkpoint的 执行时间 和 间隔多大, 下一个检查点将在上一个检查点完成后的500ms内启动
            请注意, 这意味检查点间隔永远不会小于此参数。
            防止出现例如状态数据过大而导致Checkpoint执行时间过长，从而导致Checkpoint积压过多，
            最终Flink应用密切触发Checkpoint操作，会占用了大量计算资源而影响到整个应用的性能（单位：毫秒）
         */
        checkpointConfig.setMinPauseBetweenCheckpoints(500);

        // 设置超时时间, 若本次checkpoint时间超时, 则放弃本次checkpoint操作
        checkpointConfig.setCheckpointTimeout(60000);

        // 最大并发的 Checkpoint 数: 同一时间最多可以进行多少个 checkpoint, 默认情况下, 当一个检查点仍处于运行状态时, 系统不会触发另一个检查点
        // 根据用户指定的数量可以同时触发多个Checkpoint，进而提升Checkpoint整体的效率
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        // 开启 checkpoints 的外部持久化，但是在job失败的时候不会自动清理，需要自己手工清理 state
        // 不会在任务正常停止的过程中清理掉检查点数据，而是会一直保存在外部系统介质中，另外也可以通过从外部检查点中对任务进行恢复
        // DELETE_ON_CANCELLATION：在 job canceled 时 会自动删除外部的状态数据，但是如果是 FAILED 的状态则会保留；
        // RETAIN_ON_CANCELLATION：在 job canceled 时 会保留状态数据
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置可以允许的checkpoint失败数
        checkpointConfig.setTolerableCheckpointFailureNumber(3);

        // 默认使用内存的方式存储状态值。单次快照的状态上限内存为 10MB, 使用同步方式进行快照。
        environment.setStateBackend(new MemoryStateBackend(10 * 1024 * 1024, false));

        // 使用 FsStateBackend的方式进行存储, 并且是同步方式进行快照
        environment.setStateBackend(new FsStateBackend("hdfs://hadoop-master:9000/flink/ck", false));

        DataStreamSource<String> dataStreamSource = environment.socketTextStream("hadoop-slave3", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> pairStream = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(" ");
                for (String word : split) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = pairStream.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);
        sum.print();
        environment.execute("checkpoint-test");
    }
}
