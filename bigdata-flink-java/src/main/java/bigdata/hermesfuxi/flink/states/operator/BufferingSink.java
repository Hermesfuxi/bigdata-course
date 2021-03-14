package bigdata.hermesfuxi.flink.states.operator;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

public class BufferingSink implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {
    // 缓存阈值
    private final int threshold;
    // 缓存元素
    private List<Tuple2<String, Integer>> bufferedElements;

    private transient ListState<Tuple2<String, Integer>> checkPointedState;

    public BufferingSink(int threshold) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList<>();
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        bufferedElements.add(value);
        if(bufferedElements.size() == threshold){
            for (Tuple2<String, Integer> bufferedElement : bufferedElements) {
                System.out.println(bufferedElement);
            }
            bufferedElements.clear();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkPointedState.clear();
        checkPointedState.update(bufferedElements);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple2<String, Integer>> descriptor = new ListStateDescriptor<>(
                "bufferedElements",
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {})
        );
        // 还原时使用 联合重新分发 方案（每个运算符都会获得完整的状态元素列表）
//        checkPointedState = context.getOperatorStateStore().getUnionListState(descriptor);
        // 还原时使用 基本偶分裂再分配 方案（列表被平均分成与并行运算符一样多的子列表。每个运算符都获得一个子列表，该子列表可以为空，或包含一个或多个元素。）
        checkPointedState = context.getOperatorStateStore().getListState(descriptor);
    }
}
