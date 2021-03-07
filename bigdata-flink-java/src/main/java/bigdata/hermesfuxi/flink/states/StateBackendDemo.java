package bigdata.hermesfuxi.flink.states;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *  StateBackend：保存State的存储后端，flink提供不同的状态后端（state backends）来区分状态的存储方式和存储位置
 *   持久化策略：MemoryStateBackend（默认）、FsStateBackend、RocksDBStateBackend
 *
 */
public class StateBackendDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));

        //CheckPointing 默认重启策略为 固定延迟无限重启
        environment.enableCheckpointing(5000);

        // MemoryStateBackend/FsStateBackend 默认通过配置来使用异步快照（asynchronous snapshots）避免阻塞管道（blocking pipelines）, 即 asynchronousSnapshots = true

        //一、MemoryStateBackend: 用于本地调试 或 任务状态数据量较小的场景
        // 将状态（state）数据作为对象保存在java堆内存中（taskManager），通过checkpoint机制，将状态（state）进行快照并保存JobManager（master）的堆内存中
        // 默认情况下，flink的状态会保存在 taskManager 的内存中，而 checkpoint 会保存在 jobManager 的内存中
//        environment.setStateBackend(new MemoryStateBackend());
//        environment.setStateBackend(new MemoryStateBackend(false));
//        environment.setStateBackend(new MemoryStateBackend(5 * 1024 * 1024));
        environment.setStateBackend(new MemoryStateBackend(5 * 1024 * 1024, false));

        // 二、FsStateBackend：用于大状态、长窗口、大key/value状态的的任务，全高可用配置， 保存工作状态在内存
        // 通过配置文件系统路径(type, address, path)来进行设置
        // 将动态数据保存在taskManger的内存中，通过checkpoint机制，将状态快照写入配置好的文件系统或目录中。最小元数据保存jobManager的内存中，
        // 另外可通过配置一个fileStateThreshold阈值，小于该值时state存储到 metadata 中而非文件中。
        environment.setStateBackend(new FsStateBackend("file:///D:/WorkSpaces/IdeaProjects/bigdata-course/.ck/flink"));
//        environment.setStateBackend(new FsStateBackend("hdfs://hadoop-master:9000/flink/ck"));

        // 三、RocksDBStateBackend：大状态、长窗口、大key/value状态的的任务，全高可用配置，将工作状态存储在taskManger的本地文件系统，用于生产
        // RocksDBStateBackend仅支持异步快照
        // 将工作状态保存在RocksDB数据库（位置在taskManager的数据目录）。通过checkpoint, 整个RocksDB数据库被复制到配置的文件系统或目录中。最小元数据保存jobManager的内存中
        // RocksDBStateBackend可以通过enableIncrementalCheckpointing参数配置是否进行增量Checkpoint（而MemoryStateBackend 和 FsStateBackend不能）
        environment.setStateBackend(new RocksDBStateBackend("file:///D:/WorkSpaces/IdeaProjects/bigdata-course/.ck/flink"));
//        environment.setStateBackend(new RocksDBStateBackend("hdfs://hadoop-master:9000/flink/ck", true));


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
