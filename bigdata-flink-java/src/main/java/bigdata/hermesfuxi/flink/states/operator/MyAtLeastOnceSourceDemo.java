package bigdata.hermesfuxi.flink.states.operator;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * 自定义一个Source，可以记录偏移量，实现AtLeastOnce
 */
public class MyAtLeastOnceSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);

        DataStreamSource<String> source = env.addSource(new MyAtLeastOnceSource(".ck/flink/MyAtLeastOnceSource"));

        DataStreamSource<String> textStream = env.socketTextStream("hadoop-slave3", 8888);
        DataStream<String> result = textStream.union(source);
        result.print();
        env.execute();
    }

    private static class MyAtLeastOnceSource extends RichParallelSourceFunction<String> implements CheckpointedFunction {
        private String path;
        private boolean flag = true;
        private ListState<Long> listState;
        private long offset = 0;

        public MyAtLeastOnceSource(String path) {
            this.path = path;
        }

        /**
         * 在 open/run 方法执行之前执行
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
//            System.out.println("initializeState exec");
            // 初始化或恢复历史状态（OperatorState）
            ListStateDescriptor<Long> listStateDescriptor = new ListStateDescriptor<>("MyAtLeastOnce", Long.class);
            listState = context.getOperatorStateStore().getListState(listStateDescriptor);
            //判断状态是否已经恢复了
            if(context.isRestored()){
                Iterable<Long> longIterable = listState.get();
                for (Long aLong : longIterable) {
                    offset = aLong;
                }
            }
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
//            System.out.println("run exec");
            RuntimeContext runtimeContext = getRuntimeContext();
            int indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();
            RandomAccessFile randomAccessFile = new RandomAccessFile(String.format("%s/%s.txt", path, indexOfThisSubtask), "r");
            randomAccessFile.seek(offset);

            while (flag){
                String line = randomAccessFile.readLine();
                if(StringUtils.isNoneBlank(line)){
                    line = new String(line.getBytes(UTF_8));
                    synchronized (ctx.getCheckpointLock()){
                        offset = randomAccessFile.getFilePointer();
                        ctx.collect(indexOfThisSubtask + ".txt : " + line);
                    }
                }else {
                    Thread.sleep(1000);
                }
            }
        }

        /**
         *  在触发checkpoint时，每个subTask都会执行一次
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
//            System.out.println("snapshotState exec");
            listState.clear(); //清空老的状态
            listState.add(offset); //放入新的状态
        }

        @Override
        public void cancel() {
            System.out.println("cancel exec");
            flag = false;
        }
    }
}
