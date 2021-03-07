package bigdata.hermesfuxi.flink.windows;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class EventTimeWindowAllDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 11111);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        //老版本必须要设置时间标准（1.20之前的）
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2021-03-06 09:30:30,1
        DataStreamSource<String> lines = env.socketTextStream("hadoop-slave3", 8888);

        // 新版本
        SingleOutputStreamOperator<String> linesWithWaterMarks = lines.assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        // 使用 匿名内部类
                        .withTimestampAssigner((element, recordTimestamp) -> Long.parseLong(element.split(",")[0])));
//                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
//                            @Override
//                            public long extractTimestamp(String element, long recordTimestamp) {
//                                LocalDateTime localDateTime = LocalDateTime.parse(element.split(",")[0], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
//                                return localDateTime.toEpochSecond(ZoneOffset.ofHours(8)) * 1000;
//                            }
//                        }));

        // 旧版本使用方法
        SingleOutputStreamOperator<String> oldLinesWithWaterMarks = lines.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String element) {
                LocalDateTime localDateTime = LocalDateTime.parse(element.split(",")[0], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                return localDateTime.toEpochSecond(ZoneOffset.ofHours(8)) * 1000;
            }
        });

        //提取完EventTime后会生成WaterMark，但是数据还是原来的老样子(2021-03-06 09:30:30,1)，需要转换
        SingleOutputStreamOperator<Integer> operator = linesWithWaterMarks.map(line -> Integer.valueOf(line.split(",")[1]));
        //划分窗口
        AllWindowedStream<Integer, TimeWindow> window = operator.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));
//        AllWindowedStream<Integer, TimeWindow> window = operator.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(3)));
//        AllWindowedStream<Integer, TimeWindow> window = operator.windowAll(EventTimeSessionWindows.withGap(Time.seconds(10)));
        SingleOutputStreamOperator<Integer> result = window.sum(0);
        result.print();


        env.execute();
    }
}
