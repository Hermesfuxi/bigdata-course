package bigdata.hermesfuxi.flink.connectors.file;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.util.Objects;

/**
 * @author Hermesfuxi
 *
 * 从自定义Source中读取文件中的内容，即实现 tail -f 的功能
 * 需求：
 * 1、并行读取数据；
 * 2、获取 OperatorState 状态 + Checkpoint 机制，保证 Exactly-Once（已读数据在任务异常重启之后不会再次去读）；
 * 3、文件夹有多个.txt 文件，根据 subTask Index来读取文件。即 subTask0 读取 0.txt，subTask 1读取 1.txt
 *
 * 问题：
 * Flink实现内部状态Exactly Once的语义基本原理是：
 * 隔一段时间做一个Checkpoint，持久化记录当前上游Source处理到哪里了（如Kafka offset），以及当时本地状态的值，
 * 如果过了一会进程挂了，就把这个持久化保存的Checkpoint读出来，加载当时的状态，并从当时的位置重新开始处理，这样每条消息一定只会影响自身状态一次。
 * 但这种方式是没办法保证输出到下游Sink的数据不重复的。要想下游输出的消息不重，就需要下游Sink支持事务消息，
 * 把两次checkpoint之间输出的消息当做一个事务提交，如果新的checkpoint成功，则Commit，否则Abort。
 */
public class FileExactlyOnceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10*1000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 确保检查点之间有至少500 ms的间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        // 同一时间只允许进行一个检查点
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 检查点必须在一分钟内完成，没有完成就被丢弃
        checkpointConfig.setCheckpointTimeout(60000);
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，checkpoint有多个，可以根据实际需要恢复到指定的Checkpoint
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setStateBackend(new FsStateBackend("file:///D:/WorkSpaces/IdeaProjects/bigdata-course/.ck/flink"));

        String path = "D:\\WorkSpaces\\IdeaProjects\\bigdata-course\\data\\out\\flink\\wc1";
        File filePath = new File(path);
        if(filePath.isDirectory()){
            int length = Objects.requireNonNull(filePath.listFiles()).length;
            if(length == 0){
                throw new Exception("there is not file in this path: " + path);
            }else {
                // 根据文件夹下的文件数设置并行度，有多少文件，设多少并行
                env.setParallelism(length);
            }
        }
        DataStreamSource<String> fileSource = env.addSource(new FileExactlyOnceParallelSource(path));

        // 用来人为出现异常，触发重启策略。验证重启后是否会再次去读之前已读过的数据(Exactly-Once)
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop-slave1", 8888);
        SingleOutputStreamOperator<String> errorStream = socketTextStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if (value.contains("error")) {
                    throw new RuntimeException();
                }
                return value;
            }
        });
        DataStream<String> result = fileSource.union(errorStream);

//        result.print();
        result.addSink(new FileTwoPhaseCommitSink()).setParallelism(1);

        env.execute();
    }
}
