package bigdata.hermesfuxi.flink.connectors.jdbc;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcUpsertTableSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Flink插入跟新数据到Phoenix
 */
public class JDBCPhoenixDemo {
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


        JdbcOptions jdbcOptions = JdbcOptions.builder()
                .setDBUrl("jdbc:phoenix:192.168.10.16:2181:/hbase-unsecure;autocommit=true")
                .setDriverName("org.apache.phoenix.jdbc.PhoenixDriver")
                .setTableName("ACTION_LOG")
                .build();

        TableSchema tableSchema = TableSchema.builder()
                .field("id", DataTypes.STRING().notNull())
                .field("cnt", DataTypes.BIGINT()).build();

        JdbcUpsertTableSink sink = JdbcUpsertTableSink.builder()
                .setOptions(jdbcOptions)
                .setMaxRetryTimes(5)
                .setFlushMaxSize(1000)
                .setFlushIntervalMills(1000)
                .setTableSchema(tableSchema)
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.execute();
    }

    private static class PhoenixJDBCDialect implements JdbcDialect {

        private static final long serialVersionUID = 1L;

        @Override
        public String dialectName() {
            return null;
        }

        @Override
        public boolean canHandle(String url) {
            return url.startsWith("jdbc:phoenix:");
        }

        @Override
        public JdbcRowConverter getRowConverter(RowType rowType) {
            return null;
        }

        @Override
        public Optional<String> defaultDriverName() {
            return Optional.of("org.apache.phoenix.jdbc.PhoenixDriver");
        }

        @Override
        public Optional<String> getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
            String columns = Arrays.stream(fieldNames)
                    .collect(Collectors.joining(", "));
            String placeholders = Arrays.stream(fieldNames)
                    .map(f -> "?")
                    .collect(Collectors.joining(", "));
            return Optional.of("UPSERT INTO " + tableName +
                    "(" + columns + ")" + " VALUES (" + placeholders + ")");
        }

    }
}
