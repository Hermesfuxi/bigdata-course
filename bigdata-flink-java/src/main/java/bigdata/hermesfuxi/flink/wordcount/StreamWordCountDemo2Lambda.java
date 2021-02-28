package bigdata.hermesfuxi.flink.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class StreamWordCountDemo2Lambda {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = environment.socketTextStream(args[0], Integer.parseInt(args[1]));

        // 方式一：分开写
        SingleOutputStreamOperator<String> flatMapOperator = dataStreamSource
                .flatMap(
                        // 写类型
                        (String line, Collector<String> collector) ->
                        // 方法向下转
//                        (FlatMapFunction<String, String>) (line, collector) ->
                                Arrays.stream(line.split("\\s+")).forEach(collector::collect)
                )
                .returns(Types.STRING);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneOperator1 = flatMapOperator
                .map(s -> Tuple2.of(s, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        // 方式二：合并写
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneOperator2 = dataStreamSource
                .flatMap(
                        // 写类型
                        (String line, Collector<Tuple2<String, Integer>> collector) ->
                        // 方法向下转
//                        (FlatMapFunction<String, Tuple2<String, Integer>>) (line, collector) ->
                        Arrays.stream(line.split("\\s+")).forEach(str -> collector.collect(Tuple2.of(str, 1)))
                )
                .returns(Types.TUPLE(Types.STRING, Types.INT));


        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndOneOperator1.keyBy(tuple2 -> tuple2.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        result.print();

        environment.execute(Thread.currentThread().getStackTrace()[0].getFileName());
    }
}
