package bigdata.hermesfuxi.flink.connectors.jdbc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;
import org.apache.flink.connector.jdbc.table.JdbcDynamicOutputFormatBuilder;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSink;
import org.apache.flink.connector.jdbc.table.JdbcTableSourceSinkFactory;
import org.apache.flink.connector.jdbc.table.JdbcUpsertTableSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;

/**
 *
 */
public class JDBCMysqlDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> input = env.fromCollection(Arrays.asList(Tuple2.of("123", 25), Tuple2.of("456", 24)));
        DataStream<Row> dataStream = input.map(new RichMapFunction<Tuple2<String, Integer>, Row>() {
            @Override
            public Row map(Tuple2<String, Integer> tuple2) throws Exception {
                return Row.of(tuple2.f1, tuple2.f0);
            }
        });
        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[] {
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO
        };

        JdbcUpsertTableSink jdbcUpsertTableSink = JdbcUpsertTableSink.builder()
                .setFlushIntervalMills(30000)
                .setFlushMaxSize(10)
                .setMaxRetryTimes(3)
                .build();



        JdbcOutputFormat jdbcOutputFormat = JdbcOutputFormat.buildJdbcOutputFormat()
                .setDrivername("")
                .setDBUrl("")
                .setUsername("")
                .setPassword("")
                .setQuery("")
                .setBatchSize(10)
                .finish();
        dataStream.writeUsingOutputFormat(jdbcOutputFormat);

        dataStream.addSink(JdbcSink.sink("", new JdbcStatementBuilder<Row>() {
            @Override
            public void accept(PreparedStatement preparedStatement, Row row) throws SQLException {
                preparedStatement.setString(1, "");
                preparedStatement.execute();
            }
        }, new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName("")
                .withUrl("")
                .withUsername("")
                .withPassword("")
                .build()));

        //查询mysql
        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername("")
                .setDBUrl("")
                .setUsername("")
                .setPassword("")
                .setQuery("")
                .setRowTypeInfo(new RowTypeInfo(TypeInformation.of(String.class)))
                .finish();

        DataStreamSource<Row> result = env.createInput(jdbcInputFormat);
        result.print();

        env.execute();
    }
}
