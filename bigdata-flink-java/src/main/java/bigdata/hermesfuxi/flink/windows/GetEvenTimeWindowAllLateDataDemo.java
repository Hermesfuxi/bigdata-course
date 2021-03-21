package bigdata.hermesfuxi.flink.windows;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class GetEvenTimeWindowAllLateDataDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop-slave3", 8888);

        SingleOutputStreamOperator<String> timestampsAndWatermarks = socketTextStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((SerializableTimestampAssigner<String>) (element, recordTimestamp) -> {
                            String timeStr = element.split("\\s+")[0];
                            LocalDateTime localDateTime = LocalDateTime.parse(timeStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                            return localDateTime.toEpochSecond(ZoneOffset.ofHours(8)) * 1000;
                        })
        );

        SingleOutputStreamOperator<Integer> streamOperator = timestampsAndWatermarks.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.valueOf(value.split("\\s+")[1]);
            }
        });

        OutputTag<Integer> lateTag = new OutputTag<Integer>("late"){};
        AllWindowedStream<Integer, TimeWindow> windowAll = streamOperator
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(lateTag);

        SingleOutputStreamOperator<Integer> result = windowAll.process(new ProcessAllWindowFunction<Integer, Integer, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<Integer> elements, Collector<Integer> out) throws Exception {
                Integer sum = 0;
                for (Integer element : elements) {
                    sum += element;
                }
                out.collect(sum);
            }
        });
        DataStream<Integer> lateSideOutput = result.getSideOutput(lateTag);

        result.print();
        lateSideOutput.print();
        result.union(lateSideOutput).print();

        env.execute();
    }
}
