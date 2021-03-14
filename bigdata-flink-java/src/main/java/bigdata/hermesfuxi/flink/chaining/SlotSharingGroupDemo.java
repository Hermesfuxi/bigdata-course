package bigdata.hermesfuxi.flink.chaining;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class SlotSharingGroupDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = environment.socketTextStream("hadoop-slave3", 8888);

        SingleOutputStreamOperator<String> flatMapOperator = dataStreamSource
                .flatMap((String line, Collector<String> collector) ->
                                Arrays.stream(line.split("\\s+")).forEach(collector::collect)
                )
                .returns(Types.STRING)
                .slotSharingGroup("wordcount"); //设置资源槽共享的组名称（就近跟随原则），默认 default

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneOperator1 = flatMapOperator
                .map(s -> Tuple2.of(s, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndOneOperator1.keyBy(tuple2 -> tuple2.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.reduce((ReduceFunction<Tuple2<String, Integer>>) (value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1));

        result.print();

        environment.execute(Thread.currentThread().getStackTrace()[0].getFileName());
    }
}
