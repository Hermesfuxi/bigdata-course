package bigdata.hermesfuxi.flink.states.keyed;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class CountAverageWithAggregateStateDemo {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource = env.fromElements(
                Tuple2.of(1L, 3L),
                Tuple2.of(1L, 7L),
                Tuple2.of(2L, 4L),
                Tuple2.of(1L, 5L),
                Tuple2.of(2L, 2L),
                Tuple2.of(2L, 6L)
        );

        dataStreamSource.keyBy(t->t.f0).flatMap(new CountAverageWithAggregateState()).print();
        env.execute("TestStatefulApi");
    }

    public static class CountAverageWithAggregateState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, String>> {

        private AggregatingState<Long,String> aggregatingState;

        /**初始化*/
        @Override
        public void open(Configuration parameters) throws Exception {
            AggregatingStateDescriptor descriptor = new AggregatingStateDescriptor<>("AggregatingDescriptor", new AggregateFunction<Long, String, String>() {
                //变量初始化
                @Override
                public String createAccumulator() {
                    return "Contains";
                }

                //数据处理
                @Override
                public String add(Long value, String accumulator) {
                    return "Contains".equals(accumulator) ? accumulator + value : accumulator + "and" + value;
                }

                //返回值函数
                @Override
                public String getResult(String accumulator) {
                    return accumulator;
                }

                //在 sessionTimeWindow 时，用于合并两个窗口的值
                @Override
                public String merge(String o, String acc1) {
                    return o + "and1111" + acc1;
                }
            }, String.class);

            aggregatingState = getRuntimeContext().getAggregatingState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<Long, Long> ele, Collector<Tuple2<Long, String>> collector) throws Exception {
            aggregatingState.add(ele.f1);
            collector.collect(Tuple2.of(ele.f0,aggregatingState.get()));
        }
    }
}
