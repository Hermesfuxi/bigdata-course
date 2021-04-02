package bigdata.hermesfuxi.flink.windows.time;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 窗口累计聚合历史数据
 */
public class TumblingWindowHistorySumDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString("state.savepoints.dir", "file:///D:/WorkSpaces/IdeaProjects/bigdata-course/.ck/flink");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.enableCheckpointing(10000);

        env.setStateBackend(new RocksDBStateBackend("file:///D:/WorkSpaces/IdeaProjects/bigdata-course/.ck/flink"));

        //hello hello jerry
        DataStreamSource<String> lines = env.socketTextStream("hadoop-slave3", 8888);


        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.split("\\s+");
                for (String word : words) {
                    if (word.startsWith("error")) {
                        throw new RuntimeException("出问题了！");
                    }
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        //优化：将数据划分窗口，在窗口内聚会后再写入到Redis中
        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndOne.keyBy(0);

        //使用下面的方式，是来一条算一次，写redis会对Redis压力很大
        SingleOutputStreamOperator<Tuple2<String, Integer>> result0 = keyed.sum(1).uid("result0.sum");
        result0.print("result0");

        //划分窗口
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyed.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        //在窗口内聚和
        //sum仅会聚合当前窗口的数据，不会和历史数据进行累加
        SingleOutputStreamOperator<Tuple2<String, Integer>> result1 = window.sum(1);
        result1.print("result1").uid("result1.sum");

        //调用更加灵活的aggregate方法
        SingleOutputStreamOperator<Tuple2<String, Integer>> result2 = window.aggregate(new WindowAggFunction(), new ReduceHistoryWindowFunction());
        result2.print("result2").uid("result2.sum");

        env.execute("TumblingWindowSumDemo");
    }

    //自定义窗口聚合函数，
    //Tuple2<String, Integer> 输入的数据
    // Integer 累加数据的类型
    // Integer 当前窗口累加返回的结果
    private static class WindowAggFunction implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
            return accumulator + value.f1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    private static class ReduceHistoryWindowFunction extends ProcessWindowFunction<Integer, Tuple2<String, Integer>, Tuple, TimeWindow> {

        //定义一个KeyedState
        private transient ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            //定义一个State描述器
            ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<Integer>("count-state", Integer.class);
            //初始化或获取历史状态
            countState = getRuntimeContext().getState(valueStateDescriptor);

        }

        // 窗口触发后，每一组都会调用一次（在窗口内增量聚合后的数据）
        @Override
        public void process(Tuple tuple, Context context, Iterable<Integer> input, Collector<Tuple2<String, Integer>> out) throws Exception {
            //获取分组的单词
            String word = tuple.getField(0).toString();

            //获取当前窗口累加的次数（即 WindowAggFunction 中 getResult 返回的结果）
            Integer sum = input.iterator().next();

            Integer historyCount = countState.value();
            if (historyCount == null) {
                historyCount = 0;
            }
            sum += historyCount;

            //更新状态
            countState.update(sum);

            //输出结果
            out.collect(Tuple2.of(word, sum));
        }
    }
}
