package bigdata.hermesfuxi.flink.other;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class ChainingDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //禁用OperatorChain
        //environment.disableOperatorChaining();

        DataStreamSource<String> dataStreamSource = environment.socketTextStream(args[0], Integer.parseInt(args[1]));

        SingleOutputStreamOperator<String> flatMapOperator = dataStreamSource
                .flatMap((String line, Collector<String> collector) ->
                                Arrays.stream(line.split("\\s+")).forEach(collector::collect)
                )
                .returns(Types.STRING)
//                .disableChaining(); //将该算子前面的和后面的链都断开;
                .startNewChain(); //从该算子开始，开启一个新链

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneOperator1 = flatMapOperator
                .map(s -> Tuple2.of(s, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndOneOperator1.keyBy(tuple2 -> tuple2.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.reduce((ReduceFunction<Tuple2<String, Integer>>) (value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1));

        result.print();

        environment.execute(Thread.currentThread().getStackTrace()[0].getFileName());
    }
}
