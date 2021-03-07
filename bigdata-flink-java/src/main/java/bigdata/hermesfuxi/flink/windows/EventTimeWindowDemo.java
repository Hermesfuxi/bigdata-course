package bigdata.hermesfuxi.flink.windows;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
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

public class EventTimeWindowDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 11111);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        //老版本必须要设置时间标准（1.20之前的）
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2021-03-06 09:30:30,spark,1
        DataStreamSource<String> lines = env.socketTextStream("hadoop-slave3", 8888);

        // 新版本
        SingleOutputStreamOperator<String> linesWithWaterMarks = lines.assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) ->
                                LocalDateTime.parse(
                                        element.split(",")[0],
                                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                                ).toEpochSecond(ZoneOffset.ofHours(8)) * 1000
                        )
        );

        // 旧版本使用方法
//        SingleOutputStreamOperator<String> oldLinesWithWaterMarks = lines.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
//            @Override
//            public long extractTimestamp(String element) {
//                LocalDateTime localDateTime = LocalDateTime.parse(element.split(",")[0], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
//                return localDateTime.toEpochSecond(ZoneOffset.ofHours(8)) * 1000;
//            }
//        });

        //提取完EventTime后会生成WaterMark，但是数据还是原来的老样子(2021-03-06 09:30:30,spark,1)，需要转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapOperator = linesWithWaterMarks.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] strings = value.split(",");
                return Tuple2.of(strings[1], Integer.valueOf(strings[2]));
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = mapOperator.keyBy(t -> t.f0);

        //划分窗口
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));
//        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(2)));
//        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(5)));
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = window.sum(1);
        result.print();


        env.execute();
    }
}
