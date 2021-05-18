package bigdata.hermesfuxi.flink.tablesql;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;

public class TableStreamWordCount {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 22222);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop-slave3", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split("\\s+");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

//        wordAndOne.print("wordAndOne");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // DSL 风格
//        Table sourceTable = tableEnv.fromDataStream(wordAndOne, $("word"), $("cnt"));
//        Table wordCountTable = sourceTable
//                .groupBy($("word"))
//                .select($("word"), $("cnt").sum().as("sum_cnt"));

        // SQL 风格: 不能使用 one 作为 字段名
        tableEnv.createTemporaryView("t_word_count", wordAndOne, $("word"), $("cnt"));
        Table wordCountTable = tableEnv.sqlQuery("select word, sum(cnt) as sum_cnt from t_word_count group by word");

        wordCountTable.printSchema();

        // 可更新的数据流
        // 如果了使用 groupby，table转换为流的时候只能用toRetractDStream
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(wordCountTable, Row.class);
        retractStream.print();
        // 得到的第一个boolean型字段标识 true就是最新的数据(Insert)，false表示过期老数据(Delete)
        // 9> (true,zk,4)
        // 8> (false,count,2)

        SingleOutputStreamOperator<Row> map = retractStream.filter(t -> t.f0).map(t -> t.f1);
        map.print();

        // 最后的动态表可以转换为流进行输出
//        Table res = sourcetable.select($("word").upperCase(), $("one"));
//        DataStream<Row> appendStream = tableEnv.toAppendStream(res, Row.class);
//        appendStream.print();

        env.execute("TableStreamWordCount");
    }
}
