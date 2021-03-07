package bigdata.hermesfuxi.flink.example.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Hermesfuxi
 */
public class StreamWordCountDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = environment.socketTextStream(args[0], Integer.parseInt(args[1]));

        // 方式一：分开写
        SingleOutputStreamOperator<String> singleSplitOperator = dataStreamSource.flatMap(new FlatMapFunction<String, String>(){
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] strArr = s.split("\\s+");
                for (String str : strArr) {
                    collector.collect(str);
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneOperator1 = singleSplitOperator.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });

        // 方式二：合并写
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneOperator2 = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] strArr = s.split("\\s+");
                for (String str : strArr) {
                    collector.collect(Tuple2.of(str, 1));
                }
            }
        });

        // 方式三: 创建方法类
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneOperator3 = dataStreamSource.flatMap(new MyFlatMapFunction("\\s+"));

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndOneOperator1.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        result.print();

        environment.execute(Thread.currentThread().getStackTrace()[0].getFileName());
    }

    private static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private String splitChar;
        public MyFlatMapFunction(String splitChar) {
            this.splitChar = splitChar;
        }

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] strArr = s.split(splitChar);
            for (String str : strArr) {
                collector.collect(Tuple2.of(str, 1));
            }
        }
    }
}
