package bigdata.hermesfuxi.flink.cep;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.RichPatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CEPDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<LoginEvent> streamOperator = env.fromCollection(Arrays.asList(
                new LoginEvent("1", "192.168.0.1", "fail", "1558430842"),
                new LoginEvent("1", "192.168.0.2", "fail", "1558430843"),
                new LoginEvent("1", "192.168.0.3", "fail", "1558430844"),
                new LoginEvent("2", "192.168.10.10", "success", "1558430845")
        )).assignTimestampsAndWatermarks(
                WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((loginEvent, recordTimestamp) -> Long.parseLong(loginEvent.getEventTime()))
        );

        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("begin").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                return "fail".equals(loginEvent.getEventType());
            }
        }).next("next").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                return "success".equals(loginEvent.getEventType());
            }
        }).within(Time.seconds(10));

        PatternStream<LoginEvent> eventPatternStream = CEP.pattern(streamOperator, pattern);

        SingleOutputStreamOperator<Tuple4<String, String, String, String>> result = eventPatternStream.select(new RichPatternSelectFunction<LoginEvent, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> select(Map<String, List<LoginEvent>> map) throws Exception {
                LoginEvent first = map.getOrDefault("begin", null).iterator().next();
                LoginEvent second = map.getOrDefault("next", null).iterator().next();

                return Tuple4.of(first.getUserId(), second.getUserId(), first.getEventTime(), second.getEventTime());
            }
        });

        result.print();

        env.execute();
    }
}
