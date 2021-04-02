package bigdata.hermesfuxi.flink.windows.triggers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * 自定义 DeltaTrigger 简易 Demo: 比较上一次窗口触发计算的元素，和目前到来的元素。比较结果为一个double类型阈值。如果阈值超过DeltaTrigger配置的阈值，则触发
 */
public class DeltaTriggerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop-slave1", 8888);

        //创建DeltaTrigger
        DeltaTrigger<Tuple2<String, Double>, GlobalWindow> deltaTrigger = DeltaTrigger.<Tuple2<String, Double>, GlobalWindow>of(2.0, new DeltaFunction<Tuple2<String, Double>>() {
            @Override
            public double getDelta(Tuple2<String, Double> oldDataPoint, Tuple2<String, Double> newDataPoint) {
                return newDataPoint.f1 - oldDataPoint.f1;
            }
        },
                TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {}).createSerializer(env.getConfig())
        );

        KeyedStream<Tuple2<String, Double>, String> keyedStream = socketTextStream.map(new MapFunction<String, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(String value) throws Exception {
                String[] fields = value.split("\\W+");
                return Tuple2.of(fields[0], Double.valueOf(fields[1]));
            }
        }).keyBy(t -> t.f0);

        keyedStream.window(GlobalWindows.create())
                .trigger(deltaTrigger)
                .reduce(new ReduceFunction<Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> reduce(Tuple2<String, Double> value1, Tuple2<String, Double> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .print();

        env.execute();
    }
}
/*
输入数据如下：
a,1.0
a,2.0
a,3.0
a,4.0
a,5.0
a,6.0
a,7.0

输出数据：
11> (a,10.0)   1 2 3 4
11> (a,28.0)   1 2 3 4 5 6 7

第一个触发点 4 - 1 > 2.0
第二个触发点 7 - 4 > 2.0
*/

/*
源码分析：
DeltaTrigger具有一个DeltaFunction，该函数的逻辑需要用户自己定义。
该函数比较上一次触发计算的元素，和目前到来的元素。比较结果为一个double类型阈值。
如果阈值超过DeltaTrigger配置的阈值，会返回TriggerResult.FIRE

    public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception {
        // 获取上个元素的储存状态
        ValueState<T> lastElementState = ctx.getPartitionedState(stateDesc);
        // 确保存入第一个到来的元素
        if (lastElementState.value() == null) {
            lastElementState.update(element);
            return TriggerResult.CONTINUE;
        }
        if (deltaFunction.getDelta(lastElementState.value(), element) > this.threshold) {
            // 重点：只要触发条件满足的时候，才会更新lastElementState，使用新的element替代上一个element
            lastElementState.update(element);
            // 触发计算
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

 */
