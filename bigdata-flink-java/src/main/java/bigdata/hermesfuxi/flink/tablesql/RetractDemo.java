package bigdata.hermesfuxi.flink.tablesql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class RetractDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        // use blink planner in streaming mode
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 用fromElements模拟非回撤消息
        DataStream<Tuple2<String, Integer>> dataStream = env.fromElements(
                new Tuple2<>("hello", 1),
                new Tuple2<>("hello", 1),
                new Tuple2<>("hello", 1)
        );

        tEnv.createTemporaryView("tmp", dataStream, $("word"), $("num"));

        Table table = tEnv.sqlQuery("select word, num as cnt from tmp");
//        Table table = tEnv.sqlQuery("select cnt, count(word) as freq from (select word, count(num) as cnt from tmpTable group by word) group by cnt");


        // 启用回撤流机制
        tEnv.toRetractStream(table, Row.class).print();

        env.execute();
    }
}
