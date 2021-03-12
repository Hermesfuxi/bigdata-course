package bigdata.hermesfuxi.flink.transforms.agg;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;

public class MaxByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = environment.socketTextStream("hadoop-slave3", 8888);
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> streamOperator = source.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String value) throws Exception {
                String[] strArr = value.split(",");
                if (strArr.length < 3) {
                    return null;
                } else {
                    return Tuple3.of(strArr[0], strArr[1], Integer.valueOf(strArr[2]));
                }
            }
        }).filter(Objects::nonNull)
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT));

        KeyedStream<Tuple3<String, String, Integer>, String> keyedStream = streamOperator.keyBy(t -> t.f0);
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> result = keyedStream.reduce(new ReduceFunction<Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> value1, Tuple3<String, String, Integer> value2) throws Exception {
                if(value1.f2.equals(value2.f2)){
                    if(value1.f1.contains(value2.f1)){
                        return value1;
                    }else {
                        return Tuple3.of(value1.f0, value1.f1 + "|" + value2.f1, value1.f2);
                    }
                }else if( value1.f2 > value1.f2 ){
                    return value1;
                }else {
                    return value2;
                }
            }
        });

        result.print();


        environment.execute();
    }
}
