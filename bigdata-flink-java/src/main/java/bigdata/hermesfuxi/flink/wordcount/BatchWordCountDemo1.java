package bigdata.hermesfuxi.flink.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class BatchWordCountDemo1 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> lines = environment.readTextFile("data/words.txt");

        FlatMapOperator<String, String> flatMapOperator = lines
                .flatMap(
                        (String line, Collector<String> collector) ->
                        Arrays.stream(line.split("\\s+")).forEach(collector::collect)
                )
                .returns(Types.STRING);

        MapOperator<String, Tuple2<String, Integer>> wordAndOne = flatMapOperator
                .map(str -> Tuple2.of(str, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        AggregateOperator<Tuple2<String, Integer>> result = wordAndOne.groupBy(0).sum(1);
//        result.print();

        result.writeAsText("data/out/flink/wc1").setParallelism(1);

        // 离线计算不用执行 env.execute()
        environment.execute();
    }

}
