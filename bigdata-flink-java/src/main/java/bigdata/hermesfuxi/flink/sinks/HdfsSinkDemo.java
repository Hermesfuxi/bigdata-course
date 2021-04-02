package bigdata.hermesfuxi.flink.sinks;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

public class HdfsSinkDemo {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop-slave3", 8888);

        //构建文件滚动生成的策略
        DefaultRollingPolicy<String, String> rollingPolicy = DefaultRollingPolicy.builder()
                //30秒滚动生成一个文件
                .withRolloverInterval(30 * 1000L)
//                .withRolloverInterval(TimeUnit.SECONDS.toMillis(30))
                // 不活动间隔
                .withInactivityInterval(TimeUnit.SECONDS.toMillis(10))
                //当文件达到100m滚动生成一个文件
                .withMaxPartSize(1024L * 1024L * 100L)
                .build();

        //创建StreamingFileSink，数据以行格式写入
        StreamingFileSink<String> streamingFileSink = StreamingFileSink
                .forRowFormat(
                        //指的文件存储目录
                        new Path("hdfs://hadoop-master:9000/flink/demo/string1"),
                        //指的文件的编码
                        new SimpleStringEncoder<String>("UTF-8")
                )
                //传入文件滚动生成策略
                .withRollingPolicy(rollingPolicy)
                .build();


        //指定文件目录生成的格式
        DateTimeBucketAssigner<String> bucketAssigner = new DateTimeBucketAssigner<>(
                "yyyy-MM-dd--HH-mm",
                ZoneId.of("Asia/Shanghai"));

        //构建一个StreamingFileSink，数据使用Bulk批量写入方式，存储格式为 Parquet 列式存储
        StreamingFileSink<String> streamingParquetFileSink = StreamingFileSink.
                forBulkFormat(
                        //数据写入的目录
                        new Path("hdfs://hadoop-master:9000/flink/demo/string2"),
                        //以Parquet格式写入
                        ParquetAvroWriters.forReflectRecord(String.class)
                )
                // 文件生成策略
                .withBucketAssigner(bucketAssigner)
                .build();

        socketTextStream.addSink(streamingFileSink);

        env.execute();
    }
}
