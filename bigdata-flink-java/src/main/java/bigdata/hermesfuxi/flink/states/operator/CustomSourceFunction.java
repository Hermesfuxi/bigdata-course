package bigdata.hermesfuxi.flink.states.operator;

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Collections;
import java.util.List;

public class CustomSourceFunction extends RichParallelSourceFunction<Long> implements ListCheckpointed<Long> {
    private Long offset = 0L;
    private volatile boolean isRunning = true;

    @Override
    public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
        return Collections.singletonList(offset);
    }

    @Override
    public void restoreState(List<Long> state) throws Exception {
        for (Long s : state) {
            offset = s;
        }
    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning) {
            // output and state update are atomic
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(offset);
                offset += 1;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
