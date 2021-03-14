package bigdata.hermesfuxi.flink.connectors.file;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.eclipse.jetty.util.StringUtil;

import java.io.RandomAccessFile;
import java.util.Collections;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author Hermesfuxi
 * 自定义可并行Source(保证Exactly-Once)
 * 1、extends RichParallelSourceFunction:用于实现可并行 Source
 * 2、implements CheckpointedFunction：Checkpoint机制，用来保证 Exactly-Once
 */
public class FileExactlyOnceParallelSource extends RichParallelSourceFunction<String> implements CheckpointedFunction {
    private String path;

    public FileExactlyOnceParallelSource() {
    }

    public FileExactlyOnceParallelSource(String path) {
        this.path = path;
    }

    // 状态作为数据的备份存储，不宜作频繁的更新，最好在两个checkpoint之间保持稳定，最好在snapshot时更新状态
    private transient ListState<Long> listState;
    // 用于传递偏移量
    private Long offset = 0L;

    // 用于控制文件的读取，当任务取消时，可停止读取数据
    private boolean flag = true;

    // 需要记录读取文件的偏移量，而且是 operatorState
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        listState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("ExactlyOnceParallelFileSource", Long.class));
        // 判断状态是否恢复
        if (context.isRestored()) {
            Iterable<Long> longIterable = listState.get();
            for (Long aLong : longIterable) {
                offset = aLong;
            }
        }
    }

    // 将配置传入其中，用于控制Source的读取
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        int index = getRuntimeContext().getIndexOfThisSubtask();
        RandomAccessFile randomAccessFile = new RandomAccessFile(String.format("%s/%s", path, index + 1), "r");
        randomAccessFile.seek(offset);

        while (flag) {
            String line = randomAccessFile.readLine();
            if (StringUtil.isNotBlank(line)) {
                line = new String(line.getBytes(UTF_8));
                //多并行线程不安全问题。需要加锁
                synchronized (ctx.getCheckpointLock()) {
                    offset = randomAccessFile.getFilePointer();
                    ctx.collect(index + 1 + ".txt : " + line);
                }
            } else {
                Thread.sleep(1000);
            }
        }

    }

    // 更新状态
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        listState.clear();
        listState.update(Collections.singletonList(offset));
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
